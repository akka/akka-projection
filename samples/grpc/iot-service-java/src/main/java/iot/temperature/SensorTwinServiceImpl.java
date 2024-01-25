package iot.temperature;

import akka.actor.typed.ActorSystem;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.grpc.GrpcServiceException;
import io.grpc.Status;
import iot.temperature.proto.CurrentTemperature;
import iot.temperature.proto.GetTemperatureRequest;
import iot.temperature.proto.SensorTwinService;
import java.time.Duration;
import java.util.concurrent.CompletionStage;

public class SensorTwinServiceImpl implements SensorTwinService {
  private final ActorSystem<?> system;
  private final Duration timeout;
  private final ClusterSharding sharding;

  public SensorTwinServiceImpl(ActorSystem<?> system) {
    this.system = system;
    this.timeout = system.settings().config().getDuration("iot-service.ask-timeout");
    this.sharding = ClusterSharding.get(system);
  }

  @Override
  public CompletionStage<CurrentTemperature> getTemperature(GetTemperatureRequest in) {
    EntityRef<SensorTwin.Command> entityRef =
        sharding.entityRefFor(SensorTwin.ENTITY_KEY, in.getSensorEntityId());
    CompletionStage<Integer> response =
        entityRef.askWithStatus(SensorTwin.GetTemperature::new, timeout);
    return response.handle(
        (value, error) -> {
          if (error != null) {
            throw new GrpcServiceException(Status.INTERNAL.withDescription(error.getMessage()));
          } else {
            return CurrentTemperature.newBuilder().setTemperature(value).build();
          }
        });
  }
}
