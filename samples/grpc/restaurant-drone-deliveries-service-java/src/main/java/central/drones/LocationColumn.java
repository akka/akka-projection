package central.drones;

import akka.persistence.r2dbc.state.javadsl.AdditionalColumn;

/**
 * Write local drone control location name column for querying drone locations per control location
 */
public final class LocationColumn extends AdditionalColumn<Drone.State, String> {

  @Override
  public Class<String> fieldClass() {
    return String.class;
  }

  @Override
  public String columnName() {
    return "location";
  }

  @Override
  public Binding<String> bind(Upsert<Drone.State> upsert) {
    return AdditionalColumn.bindValue(upsert.value().locationName);
  }
}
