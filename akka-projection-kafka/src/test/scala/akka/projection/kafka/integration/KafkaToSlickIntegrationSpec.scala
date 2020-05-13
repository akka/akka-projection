/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.kafka.integration

import scala.collection.immutable
import scala.collection.immutable.Seq
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.kafka.scaladsl.Producer
import akka.projection.HandlerRecoveryStrategy
import akka.projection.ProjectionId
import akka.projection.internal.MergeableOffset
import akka.projection.kafka.KafkaSourceProvider
import akka.projection.kafka.KafkaSpecBase
import akka.projection.scaladsl.SourceProvider
import akka.projection.slick.SlickHandler
import akka.projection.slick.SlickProjection
import akka.projection.slick.SlickProjectionSpec
import akka.projection.slick.internal.SlickOffsetStore
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.Assertion
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
  final case class UserEventCount(eventType: String, count: Long)

  val user1 = "user-id-1"
  val user2 = "user-id-2"
  val user3 = "user-id-3"

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

    var fail = 0

    private class UserEventCountTable(tag: Tag) extends Table[UserEventCount](tag, "EVENTS_TYPE_COUNT") {
      def eventType = column[String]("EVENT_TYPE", O.PrimaryKey)
      def count = column[Long]("COUNT")
      def * = (eventType, count).mapTo[UserEventCount]
    }

    private val userEventCountTable = TableQuery[UserEventCountTable]

    def incrementCount(eventType: String)(implicit ec: ExecutionContext): DBIO[Done] = {
      val updateCount = sqlu"UPDATE EVENTS_TYPE_COUNT SET COUNT = COUNT + 1 WHERE EVENT_TYPE = $eventType"
      updateCount.flatMap {
        case 0 =>
          // The update statement updated no records so insert a seed record instead. If this insert fails because
          // another projection inserted it in the meantime then the envelope will be processed again based on the
          // retry policy of the `SlickHandler`
          val insert = userEventCountTable += UserEventCount(eventType, 1)
          if (doTransientFailure(eventType))
            DBIO.failed(new RuntimeException(s"Failed to insert event type: $eventType"))
          else
            insert.map(_ => Done)
        case _ => DBIO.successful(Done)
      }
    }

    def findByEventType(eventType: String): DBIO[Option[UserEventCount]] =
      userEventCountTable.filter(_.eventType === eventType).result.headOption

    def createIfNotExists: Future[Unit] =
      dbConfig.db.run(userEventCountTable.schema.createIfNotExists)
  }
}

class KafkaToSlickIntegrationSpec extends KafkaSpecBase(SlickProjectionSpec.config) {
  import KafkaToSlickIntegrationSpec._

  val dbConfig: DatabaseConfig[H2Profile] = DatabaseConfig.forConfig("akka.projection.slick", config)
  val offsetStore = new SlickOffsetStore(dbConfig.db, dbConfig.profile)
  val repository = new EventTypeCountRepository(dbConfig)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val done = for {
      _ <- offsetStore.createIfNotExists
      _ <- repository.createIfNotExists
    } yield ()
    Await.result(done, 5.seconds)
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    val recs = Await.result(offsetStore.truncate(), 5.seconds)

    println(s"deleted $recs")
  }

  "KafkaSourceProvider with Slick" must {
    "project a model and Kafka offset map to a slick db exactly once" in {
      val projectionId = ProjectionId("HappyPath", "UserEventCountProjection-1")
      pending // FIXME test failure, issue #109

      val topicName = createTopic(suffix = 0, partitions = 3, replication = 1)
      val groupId = createGroupId()

      produceEvents(topicName)

      val kafkaSourceProvider: SourceProvider[MergeableOffset[Long], ConsumerRecord[String, String]] =
        KafkaSourceProvider(system, consumerDefaults.withGroupId(groupId), Set(topicName))

      val slickProjection =
        SlickProjection.exactlyOnce(
          projectionId,
          sourceProvider = kafkaSourceProvider,
          dbConfig,
          SlickHandler[ConsumerRecord[String, String]] { envelope =>
            val userId = envelope.key()
            val eventType = envelope.value()
            val userEvent = UserEvent(userId, eventType)
            // do something with the record, payload in record.value
            repository.incrementCount(userEvent.eventType)
          })

      projectionTestKit.run(slickProjection, remainingOrDefault) {
        assertEventTypeCount()
        assertAllOffsetsObserved(projectionId, topicName)
      }
    }

    "project a model and Kafka offset map to a slick db exactly once with a retriable DBIO.failed" in {
      val projectionId = ProjectionId("OneFailure", "UserEventCountProjection-1")

      val topicName = createTopic(suffix = 1, partitions = 3, replication = 1)
      val groupId = createGroupId()

      produceEvents(topicName)

      val kafkaSourceProvider: SourceProvider[MergeableOffset[Long], ConsumerRecord[String, String]] =
        KafkaSourceProvider(system, consumerDefaults.withGroupId(groupId), Set(topicName))

      // repository will fail to insert the "AddToCart" event type once only
      var failedOnce = false
      val failingRepository = new EventTypeCountRepository(dbConfig, doTransientFailure = eventType => {
        if (!failedOnce && eventType == EventType.AddToCart) {
          failedOnce = true
          true
        } else false
      })

      val slickProjection =
        SlickProjection.exactlyOnce(
          projectionId,
          sourceProvider = kafkaSourceProvider,
          dbConfig,
          new SlickHandler[ConsumerRecord[String, String]] {
            override def process(envelope: ConsumerRecord[String, String]): slick.dbio.DBIO[Done] = {
              val userId = envelope.key()
              val eventType = envelope.value()
              val userEvent = UserEvent(userId, eventType)
              // do something with the record, payload in record.value
              failingRepository.incrementCount(userEvent.eventType)
            }

            override def onFailure(
                envelope: ConsumerRecord[String, String],
                throwable: Throwable): HandlerRecoveryStrategy =
              HandlerRecoveryStrategy.retryAndFail(retries = 1, delay = 0.millis)
          })

      projectionTestKit.run(slickProjection, remainingOrDefault) {
        assertEventTypeCount()
        assertAllOffsetsObserved(projectionId, topicName)
      }
    }
  }

  private def offsetForUser(userId: String) = userEvents.count(_.userId == userId) - 1

  private def assertAllOffsetsObserved(projectionId: ProjectionId, topicName: String) = {
    withClue("check - all offsets were seen") {
      val offset = offsetStore.readOffset[MergeableOffset[Long]](projectionId).futureValue.value
      offset shouldBe MergeableOffset(
        Map(
          s"$topicName-0" -> offsetForUser(user1),
          s"$topicName-1" -> offsetForUser(user2),
          s"$topicName-2" -> offsetForUser(user3)))
    }
  }

  private def assertEventTypeCount(eventType: String): Assertion =
    dbConfig.db.run(repository.findByEventType(eventType)).futureValue.value.count shouldBe userEvents.count(
      _.eventType == eventType)

  private def assertEventTypeCount(): Assertion = {
    withClue("check - all event type counts are correct") {
      assertEventTypeCount(EventType.Login)
      assertEventTypeCount(EventType.Search)
      assertEventTypeCount(EventType.AddToCart)
      assertEventTypeCount(EventType.CheckoutCart)
      assertEventTypeCount(EventType.Logout)
    }
  }

  def produceEvents(topicName: String): Unit = {
    for {
      (userId, events) <- userEvents.groupBy(_.userId)
      partition = userId match { // deterministically produce events across available partitions
        case `user1` => 0
        case `user2` => 1
        case `user3` => 2
      }
    } awaitProduce(produceEvents(topicName, events, partition))
  }

  def produceEvents(topic: String, range: immutable.Seq[UserEvent], partition: Int = 0): Future[Done] =
    Source(range)
      .map(e => new ProducerRecord(topic, partition, e.userId, e.eventType))
      .runWith(Producer.plainSink(producerDefaults.withProducer(testProducer)))
}
