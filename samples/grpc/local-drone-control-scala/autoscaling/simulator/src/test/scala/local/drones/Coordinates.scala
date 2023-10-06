package local.drones

import java.util.concurrent.ThreadLocalRandom

final case class Coordinates(latitude: Double, longitude: Double) {
  def toProto: common.proto.coordinates.Coordinates =
    common.proto.coordinates.Coordinates(latitude, longitude)
}

object Coordinates {
  final val EarthRadiusMetres = 6371000

  // calculate distance between coordinates in metres
  def distance(start: Coordinates, destination: Coordinates): Double = {
    unitDistance(start, destination) * EarthRadiusMetres
  }

  // calculate unit distance between coordinates
  def unitDistance(start: Coordinates, destination: Coordinates): Double = {
    val φ1 = Math.toRadians(start.latitude)
    val λ1 = Math.toRadians(start.longitude)
    val φ2 = Math.toRadians(destination.latitude)
    val λ2 = Math.toRadians(destination.longitude)

    val Δφ = φ2 - φ1
    val Δλ = λ2 - λ1

    val a = Math.sin(Δφ / 2) * Math.sin(Δφ / 2) + Math.cos(φ1) * Math.cos(φ2) * Math.sin(Δλ / 2) * Math.sin(Δλ / 2)
    2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
  }

  // calculate destination coordinates given start coordinates, initial bearing, and distance
  def destination(start: Coordinates, initialBearing: Double, distanceMetres: Double): Coordinates = {
    val φ1 = Math.toRadians(start.latitude)
    val λ1 = Math.toRadians(start.longitude)
    val θ  = Math.toRadians(initialBearing)
    val δ  = distanceMetres / EarthRadiusMetres

    val φ2 = Math.asin(Math.sin(φ1) * Math.cos(δ) + Math.cos(φ1) * Math.sin(δ) * Math.cos(θ))
    val λ2 = λ1 + Math.atan2(Math.sin(θ) * Math.sin(δ) * Math.cos(φ1), Math.cos(δ) - Math.sin(φ1) * Math.sin(φ2))

    Coordinates(Math.toDegrees(φ2), Math.toDegrees(λ2))
  }

  // calculate the intermediate coordinates on the path to a destination
  // given the fraction of the distance travelled (fraction between 0 and 1)
  def intermediate(start: Coordinates, destination: Coordinates, fraction: Double): Coordinates = {
    val φ1 = Math.toRadians(start.latitude)
    val λ1 = Math.toRadians(start.longitude)
    val φ2 = Math.toRadians(destination.latitude)
    val λ2 = Math.toRadians(destination.longitude)

    val δ = unitDistance(start, destination)

    val A = Math.sin((1 - fraction) * δ) / Math.sin(δ)
    val B = Math.sin(fraction * δ) / Math.sin(δ)

    val x = A * Math.cos(φ1) * Math.cos(λ1) + B * Math.cos(φ2) * Math.cos(λ2)
    val y = A * Math.cos(φ1) * Math.sin(λ1) + B * Math.cos(φ2) * Math.sin(λ2)
    val z = A * Math.sin(φ1) + B * Math.sin(φ2)

    val φ3 = Math.atan2(z, Math.sqrt(x * x + y * y))
    val λ3 = Math.atan2(y, x)

    Coordinates(Math.toDegrees(φ3), Math.toDegrees(λ3))
  }

  // iterate a path of intermediate coordinates between start and destination, every so many metres
  def path(start: Coordinates, destination: Coordinates, everyMetres: Double): Iterator[Coordinates] = {
    val distance = Coordinates.distance(start, destination)
    val step     = everyMetres / distance
    Iterator.unfold(0.0) { fraction =>
      if (fraction >= 1.0) None
      else {
        val next        = fraction + step
        val coordinates = if (next >= 1.0) destination else intermediate(start, destination, next)
        Some(coordinates, next)
      }
    }
  }

  // select random coordinates within a circle defined by a centre and radius
  def random(centre: Coordinates, radiusMetres: Int): Coordinates = {
    val bearing  = ThreadLocalRandom.current.nextDouble * 360
    val distance = ThreadLocalRandom.current.nextDouble * radiusMetres
    destination(centre, bearing, distance)
  }
}
