package iot.registration;

import akka.Done;
import akka.actor.typed.ActorSystem;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.grpc.GrpcServiceException;
import io.grpc.Status;
import iot.registration.proto.Empty;
import iot.registration.proto.RegisterRequest;
import iot.registration.proto.RegistrationService;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegistrationServiceImpl implements RegistrationService {
  private final Logger logger = LoggerFactory.getLogger(RegistrationServiceImpl.class);
  private final ActorSystem<?> system;
  private final Duration timeout;
  private final ClusterSharding sharding;

  public RegistrationServiceImpl(ActorSystem<?> system) {
    this.system = system;
    this.timeout = system.settings().config().getDuration("iot-service.ask-timeout");
    this.sharding = ClusterSharding.get(system);
  }

  @Override
  public CompletionStage<Empty> register(RegisterRequest in) {
    logger.info("register sensor {}", in.getSensorEntityId());
    EntityRef<Registration.Command> entityRef =
        sharding.entityRefFor(Registration.ENTITY_KEY, in.getSensorEntityId());
    CompletionStage<Done> response =
        entityRef.askWithStatus(
            replyTo ->
                new Registration.Register(
                    new Registration.SecretDataValue(in.getSecret()), replyTo),
            timeout);
    return response.handle(
        (done, error) -> {
          if (error != null) {
            throw new GrpcServiceException(Status.INTERNAL.withDescription(error.getMessage()));
          } else {
            return Empty.getDefaultInstance();
          }
        });
  }
}
