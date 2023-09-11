package iot.temperature

import scala.concurrent.Future

import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.ShardedDaemonProcess
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.persistence.query.Offset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.persistence.typed.PersistenceId
import akka.projection.Projection
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.grpc.consumer.scaladsl.EventProducerPushDestination
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.SourceProvider
import akka.util.Timeout
import org.slf4j.LoggerFactory

/**
 * Handle temperature events pushed by the edge systems.
 */
object TemperatureEvents {
  val logger = LoggerFactory.getLogger("iot.temperature.TemperatureEvents")

  val TemperatureEventsStreamId = "temperature-events"

  private val ProducerEntityType = "Sensor"

  def pushedEventsGrpcHandler(implicit system: ActorSystem[_])
      : PartialFunction[HttpRequest, Future[HttpResponse]] = {

    val destination = EventProducerPushDestination(
      TemperatureEventsStreamId,
      proto.TemperatureEventsProto.javaDescriptor.getFile :: Nil)

    EventProducerPushDestination
      .grpcServiceHandler(destination)(system)
  }

  def initPushedEventsConsumer(implicit system: ActorSystem[_]): Unit = {

    implicit val askTimeout: Timeout = Timeout.create(
      system.settings.config.getDuration("iot-service.ask-timeout"))

    val sharding = ClusterSharding(system)

    def sourceProvider(sliceRange: Range)
        : SourceProvider[Offset, EventEnvelope[proto.TemperatureRead]] =
      EventSourcedProvider.eventsBySlices[proto.TemperatureRead](
        system,
        readJournalPluginId = R2dbcReadJournal.Identifier,
        ProducerEntityType,
        sliceRange.min,
        sliceRange.max)

    def projection(
        sliceRange: Range): Projection[EventEnvelope[proto.TemperatureRead]] = {
      val minSlice = sliceRange.min
      val maxSlice = sliceRange.max
      val projectionId =
        ProjectionId("TemperatureEvents", s"temperature-$minSlice-$maxSlice")

      val handler: Handler[EventEnvelope[proto.TemperatureRead]] = {
        (envelope: EventEnvelope[proto.TemperatureRead]) =>
          logger.info(
            "Saw projected event: {} #{}: {}",
            envelope.persistenceId,
            envelope.sequenceNr,
            envelope.eventOption)

          envelope.event match {
            case proto.TemperatureRead(temperature, _) =>
              val entityId =
                PersistenceId.extractEntityId(envelope.persistenceId)
              val entityRef =
                sharding.entityRefFor(SensorTwin.EntityKey, entityId)
              entityRef.askWithStatus(
                SensorTwin.UpdateTemperature(temperature, _))
            case unknown =>
              throw new RuntimeException(
                s"Unknown event type: ${unknown.getClass}")
          }
      }

      R2dbcProjection.atLeastOnceAsync(
        projectionId,
        settings = None,
        sourceProvider(sliceRange),
        handler = () => handler)
    }

    // Split the slices into N ranges
    val numberOfSliceRanges: Int = system.settings.config
      .getInt("iot-service.temperature.projections-slice-count")
    val sliceRanges = EventSourcedProvider.sliceRanges(
      system,
      R2dbcReadJournal.Identifier,
      numberOfSliceRanges)

    ShardedDaemonProcess(system).init(
      name = "TemperatureProjection",
      numberOfInstances = sliceRanges.size,
      behaviorFactory = i => ProjectionBehavior(projection(sliceRanges(i))),
      stopMessage = ProjectionBehavior.Stop)

  }

}
