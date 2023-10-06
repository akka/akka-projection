package local.drones

import akka.serialization.jackson.CborSerializable

/**
 * Decimal degree coordinates
 */
final case class Coordinates(latitude: Double, longitude: Double) {

  import Coordinates._

  def distanceTo(other: Coordinates): Long = {
    // using the haversine formula https://en.wikipedia.org/wiki/Versine#hav
    val latitudeDistance = Math.toRadians(latitude - other.latitude)
    val longitudeDistance = Math.toRadians(longitude - other.longitude)
    val sinLatitude = Math.sin(latitudeDistance / 2)
    val sinLongitude = Math.sin(longitudeDistance / 2)
    val a = sinLatitude * sinLatitude +
      (Math.cos(Math.toRadians(latitude)) *
      Math.cos(Math.toRadians(other.latitude)) *
      sinLongitude * sinLongitude)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    (roughlyRadiusOfEarthInM * c).toLong
  }

  def toProto: common.proto.Coordinates =
    common.proto.Coordinates(latitude, longitude)

}

object Coordinates {

  private val roughlyRadiusOfEarthInM = 6371000

  def fromProto(pc: common.proto.Coordinates): Coordinates =
    Coordinates(pc.latitude, pc.longitude)

}

final case class Position(coordinates: Coordinates, altitudeMeters: Double)
    extends CborSerializable

object CoarseGrainedCoordinates {

  def fromCoordinates(location: Coordinates): CoarseGrainedCoordinates = {
    // not entirely correct, but good enough for a sample/demo
    // 435-1020m precision depending on place on earth
    CoarseGrainedCoordinates(
      Math.floor(location.latitude * 100 + 0.5d) / 100,
      Math.floor(location.longitude * 100 + 0.5d) / 100)
  }

}

final case class CoarseGrainedCoordinates(latitude: Double, longitude: Double) {
  def toProto: common.proto.Coordinates =
    common.proto.Coordinates(latitude, longitude)
}
