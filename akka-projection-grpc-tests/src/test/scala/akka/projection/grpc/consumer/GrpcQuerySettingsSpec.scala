/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.consumer

import scala.concurrent.duration._

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
      settings.keepAliveInterval shouldBe None
      settings.keepAliveTimeout shouldBe None
    }

    "parse keep alive settings from config" in {
      val config = ConfigFactory.parseString("""
        stream-id = "my-stream-id"
        keep-alive-interval = 5s
        keep-alive-timeout = 15s
      """).withFallback(defaultConfig)

      val settings = GrpcQuerySettings(config)
      settings.keepAliveInterval shouldBe Some(5.seconds)
      settings.keepAliveTimeout shouldBe Some(15.seconds)
    }

    "set keep alive settings programmatically" in {
      val settings = GrpcQuerySettings("my-stream-id")
      settings.keepAliveInterval shouldBe None
      settings.keepAliveTimeout shouldBe None

      val withKeepAlive = settings
        .withKeepAliveInterval(java.time.Duration.ofSeconds(2))
        .withKeepAliveTimeout(6.seconds)
      withKeepAlive.keepAliveInterval shouldBe Some(2.seconds)
      withKeepAlive.keepAliveTimeout shouldBe Some(6.seconds)
    }

    "set keep alive settings in any order" in {
      // validated when the GrpcReadJournal is created, not here
      val settings = GrpcQuerySettings("my-stream-id").withKeepAliveTimeout(6.seconds).withKeepAliveInterval(2.seconds)
      settings.keepAliveInterval shouldBe Some(2.seconds)
      settings.keepAliveTimeout shouldBe Some(6.seconds)
    }

    "parse explicitly disabled keep alive from config" in {
      val config = ConfigFactory.parseString("""
        stream-id = "my-stream-id"
        keep-alive-interval = off
        keep-alive-timeout = none
      """).withFallback(defaultConfig)

      val settings = GrpcQuerySettings(config)
      settings.keepAliveInterval shouldBe None
      settings.keepAliveTimeout shouldBe None
    }
  }

  private def defaultConfig =
    ConfigFactory.load().getConfig("akka.projection.grpc.consumer")

}
