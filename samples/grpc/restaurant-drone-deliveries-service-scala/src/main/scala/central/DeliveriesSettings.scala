package central

import akka.actor.typed.ActorSystem

import scala.jdk.CollectionConverters._

object DeliveriesSettings {
  def apply(system: ActorSystem[_]): DeliveriesSettings = {
    val config =
      system.settings.config.getConfig("restaurant-drone-deliveries-service")
    val locationIds =
      config.getStringList("local-drone-control.locations").asScala.toSet

    // FIXME move timeouts here as well
    DeliveriesSettings(locationIds)
  }
}

case class DeliveriesSettings(locationIds: Set[String])
