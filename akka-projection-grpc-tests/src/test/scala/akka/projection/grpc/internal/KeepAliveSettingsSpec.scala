/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.internal

import scala.concurrent.duration._

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class KeepAliveSettingsSpec extends AnyWordSpecLike with Matchers {
  import KeepAliveSettings._

  "KeepAliveSettings" must {
    "accept interval within bounds and timeout at least twice the interval" in {
      validate(None, None)
      validate(Some(1.second), None)
      validate(Some(1.hour), None)
      validate(Some(1.second), Some(2.seconds))
      validate(Some(5.seconds), Some(1.minute))
    }

    "reject interval outside bounds" in {
      intercept[IllegalArgumentException](validate(Some(Duration.Zero), None))
      intercept[IllegalArgumentException](validate(Some(999.millis), None))
      intercept[IllegalArgumentException](validate(Some(1.hour + 1.second), None))
    }

    "reject timeout without interval or below twice the interval" in {
      intercept[IllegalArgumentException](validate(None, Some(10.seconds)))
      intercept[IllegalArgumentException](validate(Some(2.seconds), Some(3999.millis)))
    }

    "clamp requested producer interval" in {
      clampInterval(0) shouldBe 1.second
      clampInterval(10) shouldBe 1.second
      clampInterval(5000) shouldBe 5.seconds
      clampInterval(1.day.toMillis) shouldBe 1.hour
      clampInterval(Long.MaxValue) shouldBe 1.hour
    }
  }
}
