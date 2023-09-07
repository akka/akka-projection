package central;

import akka.actor.typed.ActorSystem;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

public final class DeliveriesSettings {
  public final Set<String> locationIds;
  public final Duration droneAskTimeout;
  public final Duration restaurantDeliveriesAskTimeout;


  public DeliveriesSettings(Set<String> locationIds, Duration droneAskTimeout, Duration restaurantDeliveriesAskTimeout) {
    this.locationIds = locationIds;
    this.droneAskTimeout = droneAskTimeout;
    this.restaurantDeliveriesAskTimeout = restaurantDeliveriesAskTimeout;
  }

  public static DeliveriesSettings create(ActorSystem<?> system) {
    var config = system.settings().config().getConfig("restaurant-drone-deliveries-service");
    var locationIds = new HashSet<>(config.getStringList("local-drone-control.locations"));
    var droneAskTimeout = config.getDuration("drone-ask-timeout");
    var restaurantDeliveriesAskTimeout = config.getDuration("restaurant-deliveries-ask-timeout");
    return new DeliveriesSettings(locationIds, droneAskTimeout, restaurantDeliveriesAskTimeout);
  }
}
