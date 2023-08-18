package local.drones

import central.CborSerializable

// FIXME needs to be here because using cbor serialization, when switched to proto as wire this can be dropped


object Drone {
  sealed trait Event extends CborSerializable

  final case class CoarseGrainedLocationChanged(
                                                 coordinates: CoarseGrainedCoordinates)
    extends Event

}
