/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer

import akka.grpc.scaladsl.StringEntry
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class GrpcQuerySettingsSpec extends AnyWordSpecLike with Matchers {
  "The GrpcQuerySettings" should {
    "parse from config" in {
      val config = ConfigFactory.parseString(""" 
        stream-id = "my-stream-id"
        additional-request-headers {
          "x-auth-header" = "secret"
        }
      """)

      val settings = GrpcQuerySettings(config)
      settings.streamId shouldBe "my-stream-id"
      settings.additionalRequestMetadata.map(_.asList) shouldBe Some(List("x-auth-header" -> StringEntry("secret")))
    }
  }

}
