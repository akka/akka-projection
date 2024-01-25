package iot.registration;

import akka.actor.typed.ActorSystem;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.japi.function.Function;
import akka.projection.grpc.producer.EventProducerSettings;
import akka.projection.grpc.producer.javadsl.EventProducer;
import akka.projection.grpc.producer.javadsl.EventProducerSource;
import akka.projection.grpc.producer.javadsl.Transformation;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public class RegistrationEvents {

  public static Function<HttpRequest, CompletionStage<HttpResponse>> eventProducerService(
      ActorSystem<?> system) {
    Transformation transformation =
        Transformation.empty()
            .registerMapper(Registration.Registered.class, RegistrationEvents::transformRegistered);

    EventProducerSource eventProducerSource =
        new EventProducerSource(
            Registration.ENTITY_KEY.name(),
            "registration-events",
            transformation,
            EventProducerSettings.create(system));

    return EventProducer.grpcServiceHandler(system, eventProducerSource);
  }

  private static Optional<iot.registration.proto.Registered> transformRegistered(
      Registration.Registered event) {
    return Optional.of(
        iot.registration.proto.Registered.newBuilder()
            .setSecret(
                iot.registration.proto.SecretDataValue.newBuilder()
                    .setValue(event.secret.value)
                    .build())
            .build());
  }
}
