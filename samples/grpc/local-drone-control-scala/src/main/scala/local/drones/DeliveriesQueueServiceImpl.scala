package local.drones

import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ ActorRef, ActorSystem }
import akka.util.Timeout
import com.google.protobuf.empty.Empty
import local.drones.proto.DeliveriesQueueService

import scala.concurrent.Future

class DeliveriesQueueServiceImpl(
    settings: Settings,
    deliveriesQueue: ActorRef[DeliveriesQueue.Command])(
    implicit system: ActorSystem[_])
    extends DeliveriesQueueService {

  import system.executionContext
  private implicit val timeout: Timeout = settings.askTimeout

  override def getCurrentQueue(
      in: Empty): Future[proto.GetCurrentQueueResponse] = {
    val reply = deliveriesQueue.ask(DeliveriesQueue.GetCurrentState(_))

    reply.map { state =>
      proto.GetCurrentQueueResponse(
        waitingDeliveries = state.waitingDeliveries.map(waiting =>
          proto.WaitingDelivery(
            deliveryId = waiting.deliveryId,
            from = Some(waiting.from.toProto),
            to = Some(waiting.to.toProto))),
        deliveriesInProgress = state.deliveriesInProgress.map(inProgress =>
          proto.DeliveryInProgress(
            deliveryId = inProgress.deliveryId,
            droneId = inProgress.droneId)))
    }

  }
}
