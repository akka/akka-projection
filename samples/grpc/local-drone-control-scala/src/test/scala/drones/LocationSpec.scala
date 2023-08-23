package drones

import local.drones.{ CoarseGrainedCoordinates, Coordinates }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class LocationSpec extends AnyWordSpecLike with Matchers {

  "The location" should {
    "group into coarse grained location" in {
      // a few places that will end up in the same coarse grained grouping
      val location1 = Coordinates(59.31767819943913, 18.14225124120123)
      val location2 = Coordinates(59.31788065145313, 18.13915892529881)
      val location3 = Coordinates(59.31757660222458, 18.136545941483007)
      val location4 = Coordinates(59.31632017813081, 18.14026838968209)
      val otherLocation1 = Coordinates(59.315230445700365, 18.148506786010383)
      val otherLocation2 = Coordinates(59.31537336341893, 18.15001210205284)

      val coarse1 = CoarseGrainedCoordinates.fromCoordinates(location1)
      val coarse2 = CoarseGrainedCoordinates.fromCoordinates(location2)
      val coarse3 = CoarseGrainedCoordinates.fromCoordinates(location3)
      val coarse4 = CoarseGrainedCoordinates.fromCoordinates(location4)
      val coarse5 = CoarseGrainedCoordinates.fromCoordinates(otherLocation1)
      val coarse6 = CoarseGrainedCoordinates.fromCoordinates(otherLocation2)

      coarse1 should equal(coarse1)
      coarse1 should equal(coarse2)
      coarse1 should equal(coarse3)
      coarse1 should equal(coarse4)
      (coarse5 should not).equal(coarse1)
      (coarse6 should not).equal(coarse1)
      coarse5 should equal(coarse6)
    }

  }
}
