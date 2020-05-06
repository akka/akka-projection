/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.examples

import scala.collection.immutable
import scala.collection.immutable.Seq
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.kafka.ConsumerSettings
import akka.kafka.scaladsl.Producer
import akka.projection.ProjectionId
import akka.projection.internal.MergeableOffsets
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
import org.apache.kafka.common.serialization.StringDeserializer
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

  class EventTypeCountRepository(val dbConfig: DatabaseConfig[H2Profile]) {

    import dbConfig.profile.api._

    private class UserEventCountTable(tag: Tag) extends Table[UserEventCount](tag, "EVENTS_TYPE_COUNT") {
      def eventType = column[String]("EVENT_TYPE", O.PrimaryKey)
      def count = column[Long]("COUNT")
      def * = (eventType, count).mapTo[UserEventCount]
    }

    def incrementCount(eventType: String)(implicit ec: ExecutionContext) = {
      for {
        count <- findByEventType(eventType).map {
          case Some(userEventCount) => userEventCount.copy(count = userEventCount.count + 1)
          case _                    => UserEventCount(eventType, 1)
        }
        _ <- userEventCountTable.insertOrUpdate(count)
      } yield Done
    }

    def findByEventType(eventType: String): DBIO[Option[UserEventCount]] =
      userEventCountTable.filter(_.eventType === eventType).result.headOption

    private val userEventCountTable = TableQuery[UserEventCountTable]

    def readValue(eventType: String): Future[Long] = {
      // map using Slick's own EC
      implicit val ec = dbConfig.db.executor.executionContext
      val action = findByEventType(eventType).map {
        case Some(eventTypeCount) => eventTypeCount.count
        case _                    => 0
      }
      dbConfig.db.run(action)
    }

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
    offsetStore.createIfNotExists
    repository.createIfNotExists
  }

  "KafkaToSlickIntegrationSpec" must {
    "project a model and Kafka offset map to a slick db exactly once" in {
      val projectionId = ProjectionId("UserEventCountProjection", "UserEventCountProjection-1")

      val topicName = createTopic(suffix = 0, partitions = 3, replication = 1)
      val groupId = createGroupId()

      for {
        (userId, events) <- userEvents.groupBy(_.userId)
        partition = userId match { // deterministically produce events across available partitions
          case `user1` => 0
          case `user2` => 1
          case `user3` => 2
        }
      } produceEvents(topicName, events, partition).futureValue

      val consumerSettings =
        ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
          .withBootstrapServers(bootstrapServers)
          .withGroupId(groupId)

      val kafkaSourceProvider: SourceProvider[MergeableOffsets.Offset[Long], ConsumerRecord[String, String]] =
        KafkaSourceProvider(system, consumerSettings, Set(topicName))

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

      def assertEventTypeCount(eventType: String) =
        dbConfig.db.run(repository.findByEventType(eventType)).futureValue.value.count shouldBe userEvents.count(
          _.eventType == eventType)

      def offsetForUser(userId: String) = userEvents.count(_.userId == userId) - 1

      projectionTestKit.run(slickProjection, remainingOrDefault) {
        withClue("check - all event type counts are correct") {
          assertEventTypeCount(EventType.Login)
          assertEventTypeCount(EventType.Search)
          assertEventTypeCount(EventType.AddToCart)
          assertEventTypeCount(EventType.CheckoutCart)
          assertEventTypeCount(EventType.Logout)
        }

        withClue("check - all offsets were seen") {
          val offset = offsetStore.readOffset[MergeableOffsets.Offset[Long]](projectionId).futureValue.value
          offset shouldBe MergeableOffsets.Offset(
            Map(
              s"$topicName-0" -> offsetForUser(user1),
              s"$topicName-1" -> offsetForUser(user2),
              s"$topicName-2" -> offsetForUser(user3)))
        }
      }
    }
  }

  def produceEvents(topic: String, range: immutable.Seq[UserEvent], partition: Int = 0): Future[Done] =
    Source(range)
      .map(e => new ProducerRecord(topic, partition, e.userId, e.eventType))
      .runWith(Producer.plainSink(producerDefaults.withProducer(testProducer)))
}
