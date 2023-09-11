package local.drones;

import static akka.actor.typed.javadsl.AskPattern.*;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import local.drones.proto.*;

public class DeliveriesQueueServiceImpl implements DeliveriesQueueService {

  private final ActorSystem<?> system;
  private final Settings settings;
  private final ActorRef<DeliveriesQueue.Command> deliveriesQueue;

  public DeliveriesQueueServiceImpl(
      ActorSystem<?> system, Settings settings, ActorRef<DeliveriesQueue.Command> deliveriesQueue) {
    this.system = system;
    this.settings = settings;
    this.deliveriesQueue = deliveriesQueue;
  }

  @Override
  public CompletionStage<GetCurrentQueueResponse> getCurrentQueue(GetCurrentQueueRequest in) {
    var reply =
        ask(
            deliveriesQueue,
            DeliveriesQueue.GetCurrentState::new,
            settings.askTimeout,
            system.scheduler());

    return reply.thenApply(this::toProto);
  }

  private GetCurrentQueueResponse toProto(DeliveriesQueue.State state) {
    return GetCurrentQueueResponse.newBuilder()
        .addAllWaitingDeliveries(
            state.waitingDeliveries.stream().map(this::waitingToProto).collect(Collectors.toList()))
        .addAllDeliveriesInProgress(
            state.deliveriesInProgress.stream()
                .map(this::deliveryInProgressToProto)
                .collect(Collectors.toList()))
        .build();
  }

  private WaitingDelivery waitingToProto(DeliveriesQueue.WaitingDelivery waiting) {
    return WaitingDelivery.newBuilder()
        .setDeliveryId(waiting.deliveryId)
        .setFrom(waiting.from.toProto())
        .setTo(waiting.to.toProto())
        .build();
  }

  private DeliveryInProgress deliveryInProgressToProto(
      DeliveriesQueue.DeliveryInProgress inProgress) {
    return DeliveryInProgress.newBuilder()
        .setDeliveryId(inProgress.deliveryId)
        .setDroneId(inProgress.droneId)
        .build();
  }
}
