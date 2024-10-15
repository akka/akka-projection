/*
 * Copyright (C) 2020-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal.metrics

import akka.projection.HandlerRecoveryStrategy
import akka.projection.ProjectionId
import akka.projection.internal.AtLeastOnce
import akka.projection.internal.FlowHandlerStrategy
import akka.projection.internal.GroupedHandlerStrategy
import akka.projection.internal.SingleHandlerStrategy
import akka.projection.internal.metrics.tools.InMemInstrumentsRegistry
import akka.projection.internal.metrics.tools.InternalProjectionStateMetricsSpec
import akka.projection.internal.metrics.tools.InternalProjectionStateMetricsSpec.Envelope
import akka.projection.internal.metrics.tools.InternalProjectionStateMetricsSpec.TelemetryTester
import akka.projection.internal.metrics.tools.TestHandlers

class WaitTimeMetricSpec extends InternalProjectionStateMetricsSpec {

  implicit var projectionId: ProjectionId = null
  before {
    projectionId = genRandomProjectionId()
  }

  def instruments(implicit projectionId: ProjectionId) = InMemInstrumentsRegistry(system).forId(projectionId)

  val defaultNumberOfEnvelopes = 6

}

class WaitTimeMetricAtLeastOnceSpec extends WaitTimeMetricSpec {

  "A metric reporting Wait Time" must {
    " in `at-least-once` with singleHandler" must {
      "report the creation time" in {
        val numOfEnvelopes = 20
        val tt: TelemetryTester =
          new TelemetryTester(AtLeastOnce(), SingleHandlerStrategy(TestHandlers.single), numOfEnvelopes)
        runInternal(tt.projectionState) {
          instruments.creationTimestampInvocations.get should be(numOfEnvelopes)
          instruments.lastCreationTimestamp.get should be < System.currentTimeMillis()
        }
      }
      "report the creation time under failing scenarios" in {
        val single = TestHandlers.singleWithErrors(1, 1, 1, 1, 2, 2, 3, 4, 5)
        val tt = new TelemetryTester(
          AtLeastOnce(recoveryStrategy = Some(HandlerRecoveryStrategy.fail)),
          SingleHandlerStrategy(single))

        runInternal(tt.projectionState) {
          instruments.creationTimestampInvocations.get should be >= (1 + 1 + 1 + 1 + 2 + 2 + 3 + 4 + 5 + 6)
          instruments.lastCreationTimestamp.get should be < System.currentTimeMillis()
        }
      }
    }

    " in `at-least-once` with groupedHandler" must {
      "report the creation time" in {
        val tt = new TelemetryTester(AtLeastOnce(), GroupedHandlerStrategy(TestHandlers.grouped))

        runInternal(tt.projectionState) {
          instruments.creationTimestampInvocations.get should be(defaultNumberOfEnvelopes)
          instruments.lastCreationTimestamp.get should be < System.currentTimeMillis()
        }
      }
    }
    " in `at-least-once` with flowHandler" must {
      "report the creation time" in {
        val tt =
          new TelemetryTester(AtLeastOnce(), FlowHandlerStrategy[Envelope](TestHandlers.flow))

        runInternal(tt.projectionState) {
          instruments.creationTimestampInvocations.get should be(defaultNumberOfEnvelopes)
          instruments.lastCreationTimestamp.get should be < System.currentTimeMillis()
        }
      }
    }

  }
}
