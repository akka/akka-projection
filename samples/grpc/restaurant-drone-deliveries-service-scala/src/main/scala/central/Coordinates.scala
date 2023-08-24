package central

// common location representations, could be a shared library between local control and restaurant
// but to keep project structure simple we duplicate

/**
 * Decimal degree coordinates
 */
final case class Coordinates(latitude: Double, longitude: Double) {
  def toProto: common.proto.Coordinates =
    common.proto.Coordinates(latitude, longitude)
}

object Coordinates {
  def fromProto(pc: common.proto.Coordinates): Coordinates =
    Coordinates(pc.latitude, pc.longitude)

}

object CoarseGrainedCoordinates {

  def fromCoordinates(location: Coordinates): CoarseGrainedCoordinates = {
    // not entirely correct, but good enough for a sample/demo
    // 435-1020m precision depending on place on earth
    CoarseGrainedCoordinates(
      Math.floor(location.latitude * 100 + 0.5d) / 100,
      Math.floor(location.longitude * 100 + 0.5d) / 100)
  }

  def fromProto(pc: common.proto.Coordinates): CoarseGrainedCoordinates =
    CoarseGrainedCoordinates(pc.latitude, pc.longitude)

}

final case class CoarseGrainedCoordinates(latitude: Double, longitude: Double)
