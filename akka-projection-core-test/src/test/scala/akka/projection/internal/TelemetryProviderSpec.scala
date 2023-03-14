/*
 * Copyright (C) 2020-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import java.util.concurrent.atomic.AtomicReference

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.projection.ProjectionId
import akka.projection.internal.metrics.tools.InMemTelemetry
import org.scalatest.wordspec.AnyWordSpecLike

/**
 *
 */
object TelemetryProviderSpec {
  val projectionId = ProjectionId("TelemetryProviderSpec-projection", "noKey")

}

class TelemetryProviderNoopSpec extends ScalaTestWithActorTestKit("") with AnyWordSpecLike with LogCapturing {
  import TelemetryProviderSpec._
  "TelemetryProvider" must {
    "provide a Noop when no implementation is set" in {

      val telemetry = TelemetryProvider.start(projectionId, system)
      telemetry should be(NoopTelemetry)
    }
  }
}

class TelemetryProviderEmptySpec extends ScalaTestWithActorTestKit("""
    |akka.projection.telemetry.implementations = []
    |""".stripMargin) with AnyWordSpecLike with LogCapturing {
  import TelemetryProviderSpec._
  "TelemetryProvider" must {
    "provide a Noop when the implementation list is empty" in {
      val telemetry = TelemetryProvider.start(projectionId, system)
      telemetry should be(NoopTelemetry)
    }
  }
}
class TelemetryProviderSingleSpec extends ScalaTestWithActorTestKit(s"""
    |akka.projection.telemetry.implementations = [${classOf[InMemTelemetry].getName}]
    |""".stripMargin) with AnyWordSpecLike with LogCapturing {
  import TelemetryProviderSpec._
  "TelemetryProvider" must {
    "provide the request impl when a single implementation is set" in {
      val telemetry = TelemetryProvider.start(projectionId, system)
      telemetry.getClass shouldBe classOf[InMemTelemetry]

    }
  }
}
class TelemetryProviderEnsembleSpec extends ScalaTestWithActorTestKit(s"""
  |akka.projection.telemetry.implementations = [${classOf[InMemTelemetry].getName}, ${classOf[FakeTelemetry].getName}, ]
  |""".stripMargin) with AnyWordSpecLike with LogCapturing {
  import TelemetryProviderSpec._
  "TelemetryProvider" must {
    "provide an ensemble impl when multiple implementations are set" in {
      val telemetry = TelemetryProvider.start(projectionId, system)
      telemetry.getClass shouldBe classOf[EnsembleTelemetry]
    }
    "propagate the correct context in beforeProcess/afterProcess when multiple implementations are set" in {
      val telemetry = TelemetryProvider.start(projectionId, system)
      val ctxs = telemetry.beforeProcess[String]("envelope", 3L)
      telemetry.afterProcess(ctxs)
      FakeTelemetry.state.get shouldBe s"$projectionId-${system.name}-envelope-3"
    }
  }
}

object FakeTelemetry {
  val state = new AtomicReference[String]("")
}
class FakeTelemetry(projectionId: ProjectionId, system: ActorSystem[_]) extends Telemetry {

  override def failed(cause: Throwable): Unit = {}

  override def stopped(): Unit = {}

  override def beforeProcess[Envelope](envelope: Envelope, creationTimeInMillis: Long): AnyRef =
    s"$projectionId-${system.name}-${envelope.toString}-$creationTimeInMillis"

  override def afterProcess(externalContext: AnyRef): Unit = FakeTelemetry.state.set(externalContext.toString)

  override def onOffsetStored(numberOfEnvelopes: Int): Unit = {}

  override def error(cause: Throwable): Unit = {}

}
