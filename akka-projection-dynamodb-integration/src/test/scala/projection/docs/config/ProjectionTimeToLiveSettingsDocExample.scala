/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package projection.docs.config

import scala.concurrent.duration._

import akka.projection.dynamodb.DynamoDBProjectionSettings
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

object ProjectionTimeToLiveSettingsDocExample {

  val ttlConfig: Config = ConfigFactory.load(ConfigFactory.parseString("""
    //#time-to-live
    akka.projection.dynamodb.time-to-live {
      projections {
        "some-projection" {
          offset-time-to-live = 7 days
        }
        "projection-*" {
          offset-time-to-live = 14 days
        }
      }
    }
    //#time-to-live
    """))
}

class ProjectionTimeToLiveSettingsDocExample extends AnyWordSpec with Matchers {
  import ProjectionTimeToLiveSettingsDocExample._

  def dynamoDBProjectionSettings(config: Config): DynamoDBProjectionSettings =
    DynamoDBProjectionSettings(config.getConfig("akka.projection.dynamodb"))

  "Projection Time to Live (TTL) docs" should {

    "have example of setting offset-time-to-live" in {
      val settings = dynamoDBProjectionSettings(ttlConfig)

      val someTtlSettings = settings.timeToLiveSettings.projections.get("some-projection")
      someTtlSettings.offsetTimeToLive shouldBe Some(7.days)

      val ttlSettings1 = settings.timeToLiveSettings.projections.get("projection-1")
      ttlSettings1.offsetTimeToLive shouldBe Some(14.days)

      val ttlSettings2 = settings.timeToLiveSettings.projections.get("projection-2")
      ttlSettings2.offsetTimeToLive shouldBe Some(14.days)

      val defaultTtlSettings = settings.timeToLiveSettings.projections.get("other-projection")
      defaultTtlSettings.offsetTimeToLive shouldBe None
    }

  }
}
