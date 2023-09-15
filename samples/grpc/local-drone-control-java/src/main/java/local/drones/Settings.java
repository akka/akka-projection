package local.drones;

import akka.actor.typed.ActorSystem;
import java.time.Duration;

public final class Settings {

  public final String locationId;
  public final Duration askTimeout;

  private Settings(String locationId, Duration askTimeout) {
    this.locationId = locationId;
    this.askTimeout = askTimeout;
  }

  public static Settings create(ActorSystem<?> system) {
    var config = system.settings().config().getConfig("local-drone-control");

    var locationId = config.getString("location-id");
    var askTimeout = config.getDuration("ask-timeout");

    return new Settings(locationId, askTimeout);
  }
}
