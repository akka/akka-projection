/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */
package local.drones

/**
 * Decimal degree coordinates
 */
final case class Coordinates(latitude: Double, longitude: Double)

final case class Position(coordinates: Coordinates, altitudeMeters: Double)

object CoarseGrainedCoordinates {

  def fromCoordinates(location: Coordinates): CoarseGrainedCoordinates = {
    // not entirely correct, but good enough for a sample/demo
    // 435-1020m precision depending on place on earth
    CoarseGrainedCoordinates(
      Math.floor(location.latitude * 100 + 0.5d) / 100,
      Math.floor(location.longitude * 100 + 0.5d) / 100)
  }

}

final case class CoarseGrainedCoordinates(latitude: Double, longitude: Double)
