package local.drones;

import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

public final class Coordinates {

  private static final int EARTH_RADIUS_METRES = 6371000;

  public final double latitude;
  public final double longitude;

  public Coordinates(double latitude, double longitude) {
    this.latitude = latitude;
    this.longitude = longitude;
  }

  public common.proto.Coordinates toProto() {
    return common.proto.Coordinates.newBuilder()
        .setLatitude(latitude)
        .setLongitude(longitude)
        .build();
  }

  // calculate distance between coordinates in metres
  public static double distance(Coordinates start, Coordinates destination) {
    return unitDistance(start, destination) * EARTH_RADIUS_METRES;
  }

  // calculate unit distance between coordinates
  public static double unitDistance(Coordinates start, Coordinates destination) {
    double φ1 = Math.toRadians(start.latitude);
    double λ1 = Math.toRadians(start.longitude);
    double φ2 = Math.toRadians(destination.latitude);
    double λ2 = Math.toRadians(destination.longitude);

    double Δφ = φ2 - φ1;
    double Δλ = λ2 - λ1;

    double a =
        Math.sin(Δφ / 2) * Math.sin(Δφ / 2)
            + Math.cos(φ1) * Math.cos(φ2) * Math.sin(Δλ / 2) * Math.sin(Δλ / 2);
    return 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  }

  // calculate destination coordinates given start coordinates, initial bearing, and distance
  public static Coordinates destination(
      Coordinates start, double initialBearing, double distanceMetres) {
    double φ1 = Math.toRadians(start.latitude);
    double λ1 = Math.toRadians(start.longitude);
    double θ = Math.toRadians(initialBearing);
    double δ = distanceMetres / EARTH_RADIUS_METRES;

    double φ2 = Math.asin(Math.sin(φ1) * Math.cos(δ) + Math.cos(φ1) * Math.sin(δ) * Math.cos(θ));
    double λ2 =
        λ1
            + Math.atan2(
                Math.sin(θ) * Math.sin(δ) * Math.cos(φ1),
                Math.cos(δ) - Math.sin(φ1) * Math.sin(φ2));

    return new Coordinates(Math.toDegrees(φ2), Math.toDegrees(λ2));
  }

  // calculate the intermediate coordinates on the path to a destination
  // given the fraction of the distance travelled (fraction between 0 and 1)
  public static Coordinates intermediate(
      Coordinates start, Coordinates destination, double fraction) {
    double φ1 = Math.toRadians(start.latitude);
    double λ1 = Math.toRadians(start.longitude);
    double φ2 = Math.toRadians(destination.latitude);
    double λ2 = Math.toRadians(destination.longitude);

    double δ = unitDistance(start, destination);

    double A = Math.sin((1 - fraction) * δ) / Math.sin(δ);
    double B = Math.sin(fraction * δ) / Math.sin(δ);

    double x = A * Math.cos(φ1) * Math.cos(λ1) + B * Math.cos(φ2) * Math.cos(λ2);
    double y = A * Math.cos(φ1) * Math.sin(λ1) + B * Math.cos(φ2) * Math.sin(λ2);
    double z = A * Math.sin(φ1) + B * Math.sin(φ2);

    double φ3 = Math.atan2(z, Math.sqrt(x * x + y * y));
    double λ3 = Math.atan2(y, x);

    return new Coordinates(Math.toDegrees(φ3), Math.toDegrees(λ3));
  }

  // iterate a path of intermediate coordinates between start and destination, every so many metres
  public static Iterator<Coordinates> path(
      Coordinates start, Coordinates destination, double everyMetres) {
    return new Iterator<>() {
      private final double distance = Coordinates.distance(start, destination);
      private final double step = everyMetres / distance;
      private double fraction = 0.0;

      @Override
      public boolean hasNext() {
        return fraction < 1.0;
      }

      @Override
      public Coordinates next() {
        fraction = fraction + step;
        return (fraction >= 1.0) ? destination : intermediate(start, destination, fraction);
      }
    };
  }

  // select random coordinates within a circle defined by a centre and radius
  public static Coordinates random(Coordinates centre, int radiusMetres) {
    double bearing = ThreadLocalRandom.current().nextDouble() * 360;
    double distance = ThreadLocalRandom.current().nextDouble() * radiusMetres;
    return destination(centre, bearing, distance);
  }
}
