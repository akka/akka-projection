/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.producer

import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.Props
import akka.actor.typed.SpawnProtocol
import akka.actor.typed.scaladsl.LoggerOps
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join
import akka.http.scaladsl.Http
import akka.persistence.query.Offset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.persistence.typed.PersistenceId
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.grpc.TestEntity
import akka.projection.grpc.consumer.scaladsl.EventConsumer
import akka.projection.grpc.internal.FilteredPayloadMapper
import akka.projection.grpc.producer.scaladsl.ActiveEventProducer
import akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import scala.concurrent.Future

// FIXME pull out to real sample for docs etc
object ProducerPushSampleCommon {
  val streamId = "fruit_stream_id"
  val entityTypeKey = EntityTypeKey[TestEntity.Command]("Fruit")
  val grpcPort = 9588
}

object ProducerPushSampleProducer {
  import ProducerPushSampleCommon._
  val log = LoggerFactory.getLogger(getClass)

  private def config = {
    ConfigFactory
      .parseString("""
         akka.actor.provider = cluster
         # new port for each producer
         akka.remote.artery.canonical.port = 0
         akka.persistence.r2dbc {
           query {
             refresh-interval = 500 millis
             # reducing this to have quicker test, triggers backtracking earlier
             backtracking.behind-current-time = 3 seconds
           }
           journal.publish-events-number-of-topics = 2
         }
        """)
      .withFallback(ConfigFactory.load("application-h2.conf"))
  }

  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem[SpawnProtocol.Command] =
      ActorSystem(SpawnProtocol(), "ProducerPushSampleProducer", config)

    val fruits = Iterator
      .continually(
        Iterator("banana", "cumquat", "peach", "cherry", "apple", "pear", "cantaloupe", "tangerine", "papaya", "mango"))
      .flatten

    if (args.length != 3) {
      println("Usage: ProducerPushSampleProducer unique-producer-id [nr-of-entities] [events-per-entity]")
      System.exit(1)
    }
    val producerId = args(0)
    val numberOfEntities = args(1).toInt
    val numberOfEventsPerEntity = args(2).toInt

    val producerProjectionId = ProjectionId("fruit-producer", producerId)

    val cluster = Cluster(system)
    cluster.manager ! Join(cluster.selfMember.address)

    val entity = Entity(entityTypeKey)(entityContext =>
      TestEntity(PersistenceId(entityContext.entityTypeKey.name, entityContext.entityId)))

    val sharding = ClusterSharding(system)
    sharding.init(entity)

    val activeEventProducer = ActiveEventProducer[String](
      producerId,
      EventProducerSource(entityTypeKey.name, streamId, Transformation.identity, EventProducerSettings(system)),
      "127.0.0.1",
      grpcPort)
    val eventSourcedProvider =
      EventSourcedProvider.eventsBySlices[String](system, R2dbcReadJournal.Identifier, entityTypeKey.name, 0, 1023)

    system ! SpawnProtocol.Spawn(
      ProjectionBehavior(
        R2dbcProjection.atLeastOnceFlow[Offset, EventEnvelope[String]](
          producerProjectionId,
          settings = None,
          sourceProvider = eventSourcedProvider,
          handler = activeEventProducer.handler())),
      "EventPusherProjection",
      Props.empty,
      system.ignoreRef)

    val refs = (1 to numberOfEntities).map { n =>
      sharding.entityRefFor(entityTypeKey, s"${producerId}_$n")
    }

    (1 to numberOfEventsPerEntity).foreach { n =>
      val fruit = fruits.next()
      refs.foreach { ref =>
        val persist = TestEntity.Persist(s"$fruit-$n")
        log.info2("Sending {} to {}", persist, ref.entityId)
        ref.tell(persist)
        Thread.sleep(10) // slow it down a bit
      }
    }
  }
}

object ProducerPushSampleConsumer {
  import ProducerPushSampleCommon._
  val log = LoggerFactory.getLogger(getClass)
  def config =
    ConfigFactory
      .parseString(s"""
     akka.http.server.enable-http2 = on
     akka.persistence.r2dbc.connection-factory = $${akka.persistence.r2dbc.h2}
     """)
      .withFallback(ConfigFactory.load("application-h2.conf"))
      .resolve()

  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem[SpawnProtocol.Command] =
      ActorSystem(SpawnProtocol(), "ProducerPushSampleConsumer", config)
    import system.executionContext

    val consumerProjectionProvider = new FilteredPayloadMapper(
      EventSourcedProvider.eventsBySlices[String](system, R2dbcReadJournal.Identifier, entityTypeKey.name, 0, 1023))

    val consumerProjectionId = ProjectionId("fruit-consumer", "0-1023")

    system ! SpawnProtocol.Spawn(
      ProjectionBehavior(
        R2dbcProjection.atLeastOnceAsync(
          consumerProjectionId,
          settings = None,
          sourceProvider = consumerProjectionProvider,
          handler = () => {
            envelope: EventEnvelope[String] =>
              log.infoN(
                "Saw projected event: {}-{}: {}",
                envelope.persistenceId,
                envelope.sequenceNr,
                envelope.eventOption.getOrElse("filtered"))
              Future.successful(Done)
          })),
      "consumer-projection",
      Props.empty,
      system.ignoreRef)

    // consumer runs gRPC server accepting pushed events from producers
    val destination = EventConsumer.EventConsumerDestination(Set(streamId))
    val bound = Http(system)
      .newServerAt("127.0.0.1", grpcPort)
      .bind(EventConsumer.grpcServiceHandler(destination))
    bound.foreach(binding =>
      log.info2(s"Consumer listening at: {}:{}", binding.localAddress.getHostString, binding.localAddress.getPort))
  }
}
