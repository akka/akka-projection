package iot.temperature

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.pattern.StatusReply
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.state.scaladsl.DurableStateBehavior
import akka.persistence.typed.state.scaladsl.Effect
import akka.serialization.jackson.CborSerializable

object SensorTwin {
  val EntityKey = EntityTypeKey[Command]("SensorTwin")

  sealed trait Command extends CborSerializable

  final case class State(temperature: Int) extends CborSerializable

  final case class UpdateTemperature(
      temperature: Int,
      replyTo: ActorRef[StatusReply[Done]])
      extends Command

  final case class GetTemperature(replyTo: ActorRef[StatusReply[Int]])
      extends Command

  def init(system: ActorSystem[_]): Unit = {
    ClusterSharding(system).init(Entity(EntityKey)(entityContext =>
      SensorTwin(entityContext.entityId)))
  }

  def apply(entityId: String): Behavior[Command] =
    DurableStateBehavior[Command, State](
      PersistenceId(EntityKey.name, entityId),
      State(0),
      (state, cmd) =>
        cmd match {
          case UpdateTemperature(temperature, replyTo) =>
            Effect
              .persist(State(temperature))
              .thenReply(replyTo)(_ => StatusReply.Ack)

          case GetTemperature(replyTo) =>
            Effect.reply(replyTo)(StatusReply.success(state.temperature))
        })

}
