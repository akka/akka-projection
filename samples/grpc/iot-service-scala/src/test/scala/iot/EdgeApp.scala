package iot

import scala.concurrent.duration._
import java.util.concurrent.ThreadLocalRandom

import scala.concurrent.Future

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.grpc.GrpcClientSettings
import akka.persistence.Persistence
import akka.persistence.query.Offset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.grpc.consumer.GrpcQuerySettings
import akka.projection.grpc.consumer.scaladsl.GrpcReadJournal
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.producer.scaladsl.EventProducer
import akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource
import akka.projection.grpc.producer.scaladsl.EventProducerPush
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import akka.projection.scaladsl.Handler
import akka.serialization.jackson.CborSerializable
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

/**
 * Illustrates how an edge add can connect to the service.
 */
object EdgeApp {

  private class EventHandler(
      projectionId: ProjectionId,
      spawner: ActorRef[Root.StartSensorSimulator])
      extends Handler[EventEnvelope[AnyRef]] {
    private val log = LoggerFactory.getLogger(getClass)

    override def process(envelope: EventEnvelope[AnyRef]): Future[Done] = {
      envelope.event match {
        case registration.proto.Registered(secret, _) =>
          val entityId = PersistenceId.extractEntityId(envelope.persistenceId)
          log.info(
            "Consumed registered sensor {} in projection {}",
            entityId,
            projectionId.id)
          spawner ! Root.StartSensorSimulator(entityId)
          Future.successful(Done)
        case other =>
          throw new IllegalArgumentException(
            s"Unknown event ${other.getClass.getName}")
      }
    }
  }

  object Root {
    sealed trait Command
    final case class StartSensorSimulator(entityId: String) extends Command

    def apply(): Behavior[Command] =
      Behaviors.setup[Command] { context =>
        implicit val system: ActorSystem[_] = context.system

        val clientSettings = GrpcClientSettings.fromConfig(
          system.settings.config
            .getConfig("akka.projection.grpc.consumer.client"))

        def initRegistrationProjection(): Unit = {
          val eventsBySlicesQuery =
            GrpcReadJournal(
              GrpcQuerySettings(system),
              clientSettings,
              List(registration.proto.RegistrationEventsProto.javaDescriptor))

          val projectionName: String = "registration-events"
          val sliceRange = 0 to 1023
          val projectionKey =
            s"${eventsBySlicesQuery.streamId}-${sliceRange.min}-${sliceRange.max}"
          val projectionId = ProjectionId.of(projectionName, projectionKey)

          val sourceProvider = EventSourcedProvider.eventsBySlices[AnyRef](
            system,
            eventsBySlicesQuery,
            eventsBySlicesQuery.streamId,
            sliceRange.min,
            sliceRange.max)

          context.spawn(
            ProjectionBehavior(
              R2dbcProjection.atLeastOnceAsync(
                projectionId,
                None,
                sourceProvider,
                () => new EventHandler(projectionId, context.self))),
            "registrationEventsProjection")
        }

        def initTemperaturePush(): Unit = {
          // turn events into a public protocol (protobuf) type before publishing
          val eventTransformation =
            EventProducer.Transformation.empty.registerAsyncEnvelopeMapper[
              SensorSimulator.TemperatureRead,
              temperature.proto.TemperatureRead] { envelope =>
              val event = envelope.event
              Future.successful(Some(
                temperature.proto.TemperatureRead(event.temperature)))
            }

          val eventProducer = EventProducerPush[SensorSimulator.Event](
            originId = "edge",
            eventProducerSource = EventProducerSource(
              SensorSimulator.EntityTypeName,
              streamId = "temperature-events",
              eventTransformation,
              EventProducerSettings(system)),
            clientSettings)

          val maxSlice = Persistence(system).numberOfSlices - 1
          context.spawn(
            ProjectionBehavior(
              R2dbcProjection
                .atLeastOnceFlow[Offset, EventEnvelope[SensorSimulator.Event]](
                  ProjectionId("temperature-push", s"0-$maxSlice"),
                  settings = None,
                  sourceProvider =
                    EventSourcedProvider.eventsBySlices[SensorSimulator.Event](
                      system,
                      R2dbcReadJournal.Identifier,
                      eventProducer.eventProducerSource.entityType,
                      0,
                      maxSlice),
                  handler = eventProducer.handler())),
            "temperaturePushProjection")
        }

        initRegistrationProjection()
        initTemperaturePush()

        Behaviors.receiveMessage { case StartSensorSimulator(entityId) =>
          if (context.child(entityId).isEmpty)
            context.spawn(SensorSimulator(entityId), entityId)
          Behaviors.same
        }
      }
  }

  object SensorSimulator {
    val EntityTypeName = "Sensor"

    sealed trait Command

    final case object Tick extends Command

    sealed trait Event extends CborSerializable

    case object State

    final case class TemperatureRead(temperature: Int) extends Event

    def apply(entityId: String): Behavior[Command] = {
      Behaviors.setup { context =>
        context.log.info("Starting sensor simulation [{}]", entityId)
        Behaviors.withTimers { timers =>
          timers.startTimerWithFixedDelay(Tick, 3.seconds)

          EventSourcedBehavior[Command, Event, State.type](
            PersistenceId(EntityTypeName, entityId),
            State,
            (_, cmd) =>
              cmd match {
                case Tick =>
                  Effect.persist(
                    TemperatureRead(ThreadLocalRandom.current().nextInt(100)))
              },
            (_, _) => State)
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    ActorSystem(Root(), "Edge", ConfigFactory.load("edge-app"))

  }

}
