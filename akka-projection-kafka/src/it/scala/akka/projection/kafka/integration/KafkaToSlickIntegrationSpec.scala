/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.kafka.integration

import java.lang.{ Long => JLong }
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.immutable
import scala.collection.immutable.Seq
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.actor.typed.scaladsl.adapter._
import akka.kafka.scaladsl.Producer
import akka.projection.HandlerRecoveryStrategy
import akka.projection.MergeableOffset
import akka.projection.ProjectionId
import akka.projection.kafka.KafkaSpecBase
import akka.projection.kafka.scaladsl.KafkaSourceProvider
import akka.projection.scaladsl.SourceProvider
import akka.projection.slick.SlickHandler
import akka.projection.slick.SlickProjection
import akka.projection.slick.SlickProjectionSpec
import akka.projection.slick.internal.SlickOffsetStore
import akka.projection.slick.internal.SlickSettings
import akka.stream.scaladsl.Source
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.Assertion
import org.scalatest.time.Milliseconds
import org.scalatest.time.Seconds
import org.scalatest.time.Span
import slick.basic.DatabaseConfig
import slick.jdbc.H2Profile

object KafkaToSlickIntegrationSpec {
  object EventType {
    val Login = "Login"
    val Search = "Search"
    val AddToCart = "AddToCart"
    val CheckoutCart = "CheckoutCart"
    val Logout = "Logout"
  }

  final case class UserEvent(userId: String, eventType: String)
  final case class UserEventCount(projectionName: String, eventType: String, count: Long)

  val user1 = "user-id-1"
  val user2 = "user-id-2"
  val user3 = "user-id-3"
  val user4 = "user-id-4"

  val userEvents = Seq(
    UserEvent(user1, EventType.Login),
    UserEvent(user1, EventType.Search),
    UserEvent(user1, EventType.Logout),
    UserEvent(user2, EventType.Login),
    UserEvent(user2, EventType.Search),
    UserEvent(user2, EventType.AddToCart),
    UserEvent(user2, EventType.Logout),
    UserEvent(user3, EventType.Login),
    UserEvent(user3, EventType.Search),
    UserEvent(user3, EventType.AddToCart),
    UserEvent(user3, EventType.Search),
    UserEvent(user3, EventType.AddToCart),
    UserEvent(user3, EventType.CheckoutCart),
    UserEvent(user3, EventType.Logout))

  class EventTypeCountRepository(
      val dbConfig: DatabaseConfig[H2Profile],
      doTransientFailure: String => Boolean = _ => false) {

    import dbConfig.profile.api._

    private class UserEventCountTable(tag: Tag) extends Table[UserEventCount](tag, "EVENTS_TYPE_COUNT") {
      def projectionName = column[String]("PROJECTION_NAME")
      def eventType = column[String]("EVENT_TYPE")
      def count = column[Long]("COUNT")
      def pk = primaryKey("PK_PROJECTION_EVENT_TYPE", (projectionName, eventType))

      def * = (projectionName, eventType, count).mapTo[UserEventCount]
    }

    private val userEventCountTable = TableQuery[UserEventCountTable]

    def incrementCount(id: ProjectionId, eventType: String)(implicit ec: ExecutionContext): DBIO[Done] = {
      val updateCount =
        sqlu"UPDATE EVENTS_TYPE_COUNT SET COUNT = COUNT + 1 WHERE PROJECTION_NAME = ${id.name} AND EVENT_TYPE = $eventType"
      updateCount.flatMap {
        case 0 =>
          // The update statement updated no records so insert a seed record instead. If this insert fails because
          // another projection inserted it in the meantime then the envelope will be processed again based on the
          // retry policy of the `SlickHandler`
          val insert = userEventCountTable += UserEventCount(id.name, eventType, 1)
          if (doTransientFailure(eventType))
            DBIO.failed(new RuntimeException(s"Failed to insert event type: $eventType"))
          else
            insert.map(_ => Done)
        case _ => DBIO.successful(Done)
      }
    }

    def findByEventType(id: ProjectionId, eventType: String): DBIO[Option[UserEventCount]] =
      userEventCountTable.filter(t => t.projectionName === id.name && t.eventType === eventType).result.headOption

    def createIfNotExists: Future[Unit] =
      dbConfig.db.run(userEventCountTable.schema.createIfNotExists)
  }
}

class KafkaToSlickIntegrationSpec extends KafkaSpecBase(ConfigFactory.load().withFallback(SlickProjectionSpec.config)) {
  import KafkaToSlickIntegrationSpec._

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(500, Milliseconds))

  val dbConfig: DatabaseConfig[H2Profile] = DatabaseConfig.forConfig(SlickSettings.configPath, config)
  val offsetStore = new SlickOffsetStore(system.toTyped, dbConfig.db, dbConfig.profile, SlickSettings(system.toTyped))
  val repository = new EventTypeCountRepository(dbConfig)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val done = for {
      _ <- SlickProjection.createOffsetTableIfNotExists(dbConfig)
      _ <- repository.createIfNotExists
    } yield ()
    Await.result(done, 5.seconds)
  }

  "KafkaSourceProvider with Slick" must {

    "project a model and Kafka offset map to a slick db exactly once" in {
      val topicName = createTopic(suffix = 0, partitions = 3, replication = 1)
      val groupId = createGroupId()
      val projectionId = ProjectionId(groupId, "UserEventCountProjection-1")

      produceEvents(topicName)

      val kafkaSourceProvider: SourceProvider[MergeableOffset[JLong], ConsumerRecord[String, String]] =
        KafkaSourceProvider(
          system.toTyped,
          consumerDefaults
            .withGroupId(groupId),
          Set(topicName))

      val slickProjection =
        SlickProjection.exactlyOnce(
          projectionId,
          sourceProvider = kafkaSourceProvider,
          dbConfig,
          () =>
            SlickHandler[ConsumerRecord[String, String]] { envelope =>
              val userId = envelope.key()
              val eventType = envelope.value()
              val userEvent = UserEvent(userId, eventType)
              // do something with the record, payload in record.value
              repository.incrementCount(projectionId, userEvent.eventType)
            })

      projectionTestKit.run(slickProjection, remainingOrDefault) {
        assertEventTypeCount(projectionId)
        assertAllOffsetsObserved(projectionId, topicName)
      }
    }

    "project a model and Kafka offset map to a slick db exactly once with a retriable DBIO.failed" in {
      val topicName = createTopic(suffix = 1, partitions = 3, replication = 1)
      val groupId = createGroupId()
      val projectionId = ProjectionId(groupId, "UserEventCountProjection-1")

      produceEvents(topicName)

      val kafkaSourceProvider: SourceProvider[MergeableOffset[JLong], ConsumerRecord[String, String]] =
        KafkaSourceProvider(
          system.toTyped,
          consumerDefaults
            .withGroupId(groupId),
          Set(topicName))

      // repository will fail to insert the "AddToCart" event type once only
      val failedOnce = new AtomicBoolean
      val failingRepository = new EventTypeCountRepository(dbConfig, doTransientFailure = eventType => {
        if (!failedOnce.get && eventType == EventType.AddToCart) {
          failedOnce.set(true)
          true
        } else false
      })

      val slickProjection =
        SlickProjection
          .exactlyOnce(
            projectionId,
            sourceProvider = kafkaSourceProvider,
            dbConfig,
            () =>
              SlickHandler[ConsumerRecord[String, String]] { envelope =>
                val userId = envelope.key()
                val eventType = envelope.value()
                val userEvent = UserEvent(userId, eventType)
                // do something with the record, payload in record.value
                failingRepository.incrementCount(projectionId, userEvent.eventType)
              })
          .withRecoveryStrategy(HandlerRecoveryStrategy.retryAndFail(retries = 1, delay = 0.millis))

      projectionTestKit.run(slickProjection, remainingOrDefault) {
        assertEventTypeCount(projectionId)
        assertAllOffsetsObserved(projectionId, topicName)
      }
    }

    // https://github.com/akka/akka-projection/issues/382
    "resume a projection from the last saved offset plus one" in {
      val topicName = createTopic(suffix = 2, partitions = 4, replication = 1)
      val groupId = createGroupId()
      val projectionId = ProjectionId(groupId, "UserEventCountProjection-1")

      produceEvents(topicName)

      val kafkaSourceProvider: SourceProvider[MergeableOffset[JLong], ConsumerRecord[String, String]] =
        KafkaSourceProvider(
          system.toTyped,
          consumerDefaults
            .withGroupId(groupId),
          Set(topicName))

      def slickProjection() = {

        SlickProjection.exactlyOnce(
          projectionId,
          sourceProvider = kafkaSourceProvider,
          dbConfig,
          () =>
            SlickHandler[ConsumerRecord[String, String]] { envelope =>
              val userId = envelope.key()
              val eventType = envelope.value()
              val userEvent = UserEvent(userId, eventType)
              // do something with the record, payload in record.value
              repository.incrementCount(projectionId, userEvent.eventType)
            })
      }

      projectionTestKit.run(slickProjection(), remainingOrDefault) {
        assertEventTypeCount(projectionId)
        assertAllOffsetsObserved(projectionId, topicName)
      }

      // publish some more events
      Source(
        List(UserEvent(user1, EventType.Login), UserEvent(user4, EventType.Login), UserEvent(user4, EventType.Search)))
        .map(e => new ProducerRecord(topicName, partitionForUser(e.userId), e.userId, e.eventType))
        .runWith(Producer.plainSink(producerDefaults.withProducer(testProducer)))
        .futureValue

      // re-run it in order to read back persisted offsets
      projectionTestKit.run(slickProjection(), remainingOrDefault) {
        val count = dbConfig.db.run(repository.findByEventType(projectionId, EventType.Login)).futureValue.value.count
        count shouldBe (userEvents.count(_.eventType == EventType.Login) + 2)

        val offset = offsetStore.readOffset[MergeableOffset[Long]](projectionId).futureValue.value
        offset shouldBe MergeableOffset(
          Map(
            s"$topicName-0" -> (userEvents.count(_.userId == user1) + 1 - 1), // one more for user1
            s"$topicName-1" -> (userEvents.count(_.userId == user2) - 1),
            s"$topicName-2" -> (userEvents.count(_.userId == user3) - 1),
            s"$topicName-3" -> (2 - 1) // two new for user4
          ))
      }

    }
  }

  private def assertAllOffsetsObserved(projectionId: ProjectionId, topicName: String) = {
    def offsetForUser(userId: String) = userEvents.count(_.userId == userId) - 1

    withClue("check - all offsets were seen") {
      val offset = offsetStore.readOffset[MergeableOffset[Long]](projectionId).futureValue.value
      offset shouldBe MergeableOffset(
        Map(
          s"$topicName-0" -> offsetForUser(user1),
          s"$topicName-1" -> offsetForUser(user2),
          s"$topicName-2" -> offsetForUser(user3)))
    }
  }

  private def assertEventTypeCount(id: ProjectionId): Assertion = {
    def assertEventTypeCount(eventType: String): Assertion = {
      val count = dbConfig.db.run(repository.findByEventType(id, eventType)).futureValue.value.count
      count shouldBe userEvents.count(_.eventType == eventType)
    }

    withClue("check - all event type counts are correct") {
      assertEventTypeCount(EventType.Login)
      assertEventTypeCount(EventType.Search)
      assertEventTypeCount(EventType.AddToCart)
      assertEventTypeCount(EventType.CheckoutCart)
      assertEventTypeCount(EventType.Logout)
    }
  }

  def produceEvents(topicName: String): Unit = {
    val produceFs = for {
      (userId, events) <- userEvents.groupBy(_.userId)
      partition = partitionForUser(userId)
    } yield produceEvents(topicName, events, partition)
    Await.result(Future.sequence(produceFs), remainingOrDefault)
  }

  def partitionForUser(userId: String): Int = {
    userId match { // deterministically produce events across available partitions
      case `user1` => 0
      case `user2` => 1
      case `user3` => 2
      case `user4` => 3
    }
  }

  def produceEvents(topic: String, range: immutable.Seq[UserEvent], partition: Int = 0): Future[Done] =
    Source(range)
      .map(e => new ProducerRecord(topic, partition, e.userId, e.eventType))
      .runWith(Producer.plainSink(producerDefaults.withProducer(testProducer)))
}
