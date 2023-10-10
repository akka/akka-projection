package local.drones;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.Extension;
import akka.actor.typed.ExtensionId;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import java.io.IOException;

public class Telemetry implements Extension {

  public static class Id extends ExtensionId<Telemetry> {

    private static final Id instance = new Id();

    private Id() {}

    @Override
    public Telemetry createExtension(ActorSystem<?> system) {
      return new Telemetry(system);
    }

    public static Telemetry get(ActorSystem<?> system) {
      return instance.apply(system);
    }
  }

  private final int prometheusPort;

  private Telemetry(ActorSystem<?> system) {
    this.prometheusPort = system.settings().config().getInt("prometheus.port");
  }

  public void start() throws IOException {
    new HTTPServer.Builder().withPort(prometheusPort).build();
  }

  private final Gauge activeDroneEntities =
      Gauge.build()
          .name("local_drone_control_active_entities")
          .help("Number of currently active drone entities.")
          .register();

  public void droneEntityActivated() {
    activeDroneEntities.inc();
  }

  public void droneEntityPassivated() {
    activeDroneEntities.dec();
  }
}
