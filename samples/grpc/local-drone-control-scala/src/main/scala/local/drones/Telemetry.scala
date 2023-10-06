package local.drones

import akka.actor.typed.{ ActorSystem, Extension, ExtensionId }
import io.prometheus.client.Gauge
import io.prometheus.client.exporter.HTTPServer

object Telemetry extends ExtensionId[Telemetry] {
  override def createExtension(system: ActorSystem[_]): Telemetry = {
    new Telemetry(system)
  }
}

final class Telemetry(system: ActorSystem[_]) extends Extension {

  def start(): Unit = {
    val port = system.settings.config.getInt("prometheus.port")
    new HTTPServer.Builder().withPort(port).build()
  }

  private val activeDroneEntities: Gauge = Gauge.build
    .name("local_drone_control_active_entities")
    .help("Number of currently active drone entities.")
    .register()

  def droneEntityActivated(): Unit = activeDroneEntities.inc()

  def droneEntityPassivated(): Unit = activeDroneEntities.dec()

}
