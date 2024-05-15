package iot.temperature;

import akka.Done;
import akka.actor.typed.ActorSystem;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.ShardedDaemonProcess;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.japi.Pair;
import akka.japi.function.Function;
import akka.persistence.query.Offset;
import akka.persistence.query.typed.EventEnvelope;
import akka.persistence.r2dbc.query.javadsl.R2dbcReadJournal;
import akka.persistence.typed.PersistenceId;
import akka.projection.Projection;
import akka.projection.ProjectionBehavior;
import akka.projection.ProjectionId;
import akka.projection.eventsourced.javadsl.EventSourcedProvider;
import akka.projection.grpc.consumer.javadsl.EventProducerPushDestination;
import akka.projection.grpc.consumer.javadsl.Transformation;
import akka.projection.javadsl.Handler;
import akka.projection.javadsl.SourceProvider;
import akka.projection.r2dbc.javadsl.R2dbcProjection;
import iot.temperature.proto.TemperatureRead;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TemperatureEvents {

  private static final Logger logger = LoggerFactory.getLogger(TemperatureEvents.class);

  public static final String TEMPERATURE_EVENT_STREAM_ID = "temperature-events";

  private static final String PRODUCER_ENTITY_TYPE = "Sensor";

  public static Function<HttpRequest, CompletionStage<HttpResponse>> pushedEventsGrpcHandler(
      ActorSystem<?> system) {
    var destination =
        EventProducerPushDestination.create(
                TEMPERATURE_EVENT_STREAM_ID,
                Collections.singletonList(iot.temperature.proto.TemperatureEvents.getDescriptor()),
                system);

    return EventProducerPushDestination.grpcServiceHandler(destination, system);
  }

  private static class TemperatureReadHandler extends Handler<EventEnvelope<TemperatureRead>> {

    private final ClusterSharding sharding;
    private final Duration askTimeout;

    public TemperatureReadHandler(ActorSystem<?> system) {
      this.sharding = ClusterSharding.get(system);
      this.askTimeout = system.settings().config().getDuration("iot-service.ask-timeout");
    }

    @Override
    public CompletionStage<Done> process(EventEnvelope<TemperatureRead> envelope) {
      logger.info(
          "Saw projected event: {}-{}: {}",
          envelope.persistenceId(),
          envelope.sequenceNr(),
          envelope.eventOption());

      var entityId = PersistenceId.extractEntityId(envelope.persistenceId());

      var entityRef = sharding.entityRefFor(SensorTwin.ENTITY_KEY, entityId);

      return entityRef.askWithStatus(
          replyTo -> new SensorTwin.UpdateTemperature(envelope.event().getTemperature(), replyTo),
          askTimeout);
    }
  }

  public static void initPushedEventsConsumer(ActorSystem<?> system) {
    // Split the slices into N ranges
    var numberOfSliceRanges =
        system.settings().config().getInt("iot-service.temperature.projections-slice-count");

    var sliceRanges =
        EventSourcedProvider.sliceRanges(
            system, R2dbcReadJournal.Identifier(), numberOfSliceRanges);

    ShardedDaemonProcess.get(system)
        .init(
            ProjectionBehavior.Command.class,
            "TemperatureProjection",
            sliceRanges.size(),
            i -> ProjectionBehavior.create(projection(system, sliceRanges.get(i))),
            ProjectionBehavior.stopMessage());
  }

  private static Projection<EventEnvelope<TemperatureRead>> projection(
      ActorSystem<?> system, Pair<Integer, Integer> sliceRange) {
    var minSlice = sliceRange.first();
    var maxSlice = sliceRange.second();
    var projectionId =
        ProjectionId.of("TemperatureEvents", "temperature-" + minSlice + "-" + maxSlice);

    SourceProvider<Offset, EventEnvelope<TemperatureRead>> sourceProvider =
        EventSourcedProvider.eventsBySlices(
            system,
            R2dbcReadJournal.Identifier(),
            PRODUCER_ENTITY_TYPE,
            sliceRange.first(),
            sliceRange.second());

    return R2dbcProjection.atLeastOnceAsync(
        projectionId,
        Optional.empty(),
        sourceProvider,
        () -> new TemperatureReadHandler(system),
        system);
  }
}
