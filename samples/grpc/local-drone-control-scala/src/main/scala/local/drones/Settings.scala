package local.drones

import akka.actor.typed.ActorSystem
import akka.util.Timeout

import scala.jdk.DurationConverters.JavaDurationOps

object Settings {
  def apply(system: ActorSystem[_]): Settings = {
    val config = system.settings.config.getConfig("local-drone-control")

    val locationId = config.getString("location-id")
    val askTimeout = config.getDuration("ask-timeout").toScala

    Settings(locationId, askTimeout)
  }
}

final case class Settings(locationId: String, askTimeout: Timeout)
