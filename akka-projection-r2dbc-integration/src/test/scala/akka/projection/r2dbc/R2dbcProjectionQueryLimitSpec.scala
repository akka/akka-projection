/*
 * Copyright (C) 2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.r2dbc

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.projection.r2dbc.internal.R2dbcProjectionImpl
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.r2dbc.spi.R2dbcNonTransientResourceException
import org.scalatest.wordspec.AnyWordSpecLike

object R2dbcProjectionQueryLimitSpec {
  // close-calls-exceeding keeps its default of 20 seconds
  val config: Config = ConfigFactory
    .parseString("""
    akka.persistence.r2dbc.connection-factory.close-queries-exceeding = 1s
    """)
    .withFallback(TestConfig.config)
}

class R2dbcProjectionQueryLimitSpec
    extends ScalaTestWithActorTestKit(R2dbcProjectionQueryLimitSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system

  "The executor of an R2DBC projection" must {

    "limit a query with close-queries-exceeding of the connection factory" in {
      // H2 has no pg_sleep
      if (r2dbcSettings.dialectName == "h2")
        pending

      val executor = R2dbcProjectionImpl.createExecutor(r2dbcProjectionSettings)

      val result = LoggingTestKit.warn("Timeout after [1 second]").expect {
        executor.select("test query limit")(_.createStatement("SELECT pg_sleep(5)"), _ => Done)
      }

      result.failed.futureValue shouldBe a[R2dbcNonTransientResourceException]
    }
  }
}
