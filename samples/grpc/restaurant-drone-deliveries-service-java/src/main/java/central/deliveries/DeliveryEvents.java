package central.deliveries;

import akka.actor.typed.ActorSystem;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.japi.function.Function;
import akka.persistence.query.typed.EventEnvelope;
import akka.projection.grpc.producer.EventProducerSettings;
import akka.projection.grpc.producer.javadsl.EventProducer;
import akka.projection.grpc.producer.javadsl.EventProducerSource;
import akka.projection.grpc.producer.javadsl.Transformation;
import central.deliveries.proto.DeliveryRegistered;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public final class DeliveryEvents {

  public static Function<HttpRequest, CompletionStage<HttpResponse>> eventProducerService(
      ActorSystem<?> system) {
    var transformation =
        Transformation.empty()
            .registerAsyncEnvelopeMapper(
                RestaurantDeliveries.DeliveryRegistered.class,
                DeliveryEvents::transformDeliveryRegistration)
            // exclude all other types of events for the RestaurantDeliveries
            .registerOrElseMapper(envelope -> Optional.empty());

    var eventProducerSource =
        new EventProducerSource(
            RestaurantDeliveries.ENTITY_KEY.name(),
            // Note: stream id used in consumer to consume this specific stream
            "delivery-events",
            transformation,
            EventProducerSettings.create(system));

    return EventProducer.grpcServiceHandler(system, eventProducerSource);
  }

  private static CompletionStage<Optional<DeliveryRegistered>> transformDeliveryRegistration(
      EventEnvelope<RestaurantDeliveries.DeliveryRegistered> envelope) {
    var delivery = envelope.event().delivery;
    return CompletableFuture.completedFuture(
        Optional.of(
            central.deliveries.proto.DeliveryRegistered.newBuilder()
                .setDeliveryId(delivery.deliveryId)
                .setOrigin(delivery.origin.toProto())
                .setDestination(delivery.destination.toProto())
                .build()));
  }
}
