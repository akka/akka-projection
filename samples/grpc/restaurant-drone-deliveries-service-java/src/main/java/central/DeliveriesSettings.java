package central;

import akka.actor.typed.ActorSystem;
import java.util.HashSet;
import java.util.Set;

public final class DeliveriesSettings {
  public final Set<String> locationIds;

  public DeliveriesSettings(Set<String> locationIds) {
    this.locationIds = locationIds;
  }

  public static DeliveriesSettings create(ActorSystem<?> system) {
    var config = system.settings().config().getConfig("restaurant-drone-deliveries-service");
    var locationIds = new HashSet<>(config.getStringList("local-drone-control.locations"));

    // FIXME move timeouts here as well
    return new DeliveriesSettings(locationIds);
  }
}
