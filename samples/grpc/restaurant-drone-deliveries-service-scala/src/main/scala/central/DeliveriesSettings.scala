package central

import akka.actor.typed.ActorSystem

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._

object DeliveriesSettings {
  def apply(system: ActorSystem[_]): DeliveriesSettings = {
    val config =
      system.settings.config.getConfig("restaurant-drone-deliveries-service")
    val locationIds =
      config.getStringList("local-drone-control.locations").asScala.toSet
    val droneAskTimeout = config.getDuration("drone-ask-timeout").toScala
    val restaurantDeliveriesAskTimeout =
      config.getDuration("restaurant-deliveries-ask-timeout").toScala

    DeliveriesSettings(
      locationIds,
      droneAskTimeout,
      restaurantDeliveriesAskTimeout)
  }
}

final case class DeliveriesSettings(
    locationIds: Set[String],
    droneAskTimeout: FiniteDuration,
    restaurantDeliveriesAskTimeout: FiniteDuration)
