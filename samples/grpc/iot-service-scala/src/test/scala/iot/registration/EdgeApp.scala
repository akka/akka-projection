package iot.registration

import scala.concurrent.Future

import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.grpc.GrpcClientSettings
import akka.persistence.query.typed.EventEnvelope
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.grpc.consumer.GrpcQuerySettings
import akka.projection.grpc.consumer.scaladsl.GrpcReadJournal
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import akka.projection.scaladsl.Handler
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

/**
 * Illustrates how an edge add can connect to the service.
 */
object EdgeApp {

  private class EventHandler(projectionId: ProjectionId)
      extends Handler[EventEnvelope[AnyRef]] {
    private val log = LoggerFactory.getLogger(getClass)

    override def process(envelope: EventEnvelope[AnyRef]): Future[Done] = {
      envelope.event match {
        case proto.Registered(sensorId, secret, _) =>
          log.info(
            "Consumed registered sensor {} in projection {}",
            sensorId,
            projectionId.id)
          Future.successful(Done)
        case other =>
          throw new IllegalArgumentException(
            s"Unknown event ${other.getClass.getName}")
      }
    }
  }

  object Root {
    def apply(): Behavior[Nothing] =
      Behaviors.setup[Nothing] { context =>
        implicit val system: ActorSystem[_] = context.system

        val clientSettings = GrpcClientSettings
          .fromConfig(
            system.settings.config.getConfig(
              "akka.projection.grpc.consumer.client"))

        val eventsBySlicesQuery =
          GrpcReadJournal(
            GrpcQuerySettings(system),
            clientSettings,
            List(proto.RegistrationEventsProto.javaDescriptor))

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
              () => new EventHandler(projectionId))),
          "registrationEventsProjection")

        Behaviors.empty
      }
  }

  def main(args: Array[String]): Unit = {
    ActorSystem[Nothing](Root(), "Edge", ConfigFactory.load("edge-app"))

  }

}
