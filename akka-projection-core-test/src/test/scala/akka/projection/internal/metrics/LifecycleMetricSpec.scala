/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
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

class LifecycleMetricSpec extends InternalProjectionStateMetricsSpec {

  implicit var projectionId: ProjectionId = null
  before {
    projectionId = genRandomProjectionId()
  }

  def instruments(implicit projectionId: ProjectionId) = InMemInstrumentsRegistry(system).forId(projectionId)

  val defaultNumberOfEnvelopes = 6

}
class LifecycleMetricAtLeastOnceSpec extends LifecycleMetricSpec {

  "A metric reporting projection lifecycle metrics" must {
    // at-least-once
    " in `at-least-once` with singleHandler" must {
      "count a start and a stop" in {
        val numOfEnvelopes = 20
        val tt: TelemetryTester =
          new TelemetryTester(AtLeastOnce(), SingleHandlerStrategy(TestHandlers.single), numOfEnvelopes)
        runInternal(tt.projectionState) {
          instruments.offsetsSuccessfullyCommitted.get should be(numOfEnvelopes)
        }
        eventually {
          instruments.startedInvocations.get should be(1)
          instruments.stoppedInvocations.get should be(1)
        }
      }
      "count projection failures" in {
        val single = TestHandlers.singleWithErrors(1, 1, 1, 1, 2, 2, 3, 4, 5)
        val tt = new TelemetryTester(
          AtLeastOnce(recoveryStrategy = Some(HandlerRecoveryStrategy.fail)),
          SingleHandlerStrategy(single))

        runInternal(tt.projectionState) {
          instruments.offsetsSuccessfullyCommitted.get should be(defaultNumberOfEnvelopes)
        }
        eventually {
          instruments.startedInvocations.get should be(10)
          instruments.stoppedInvocations.get should be(10)
          instruments.failureInvocations.get should be(9)
        }
      }
    }

    " in `at-least-once` with groupedHandler" must {
      "report nothing in happy scenarios" in {
        val tt = new TelemetryTester(AtLeastOnce(), GroupedHandlerStrategy(TestHandlers.grouped))

        runInternal(tt.projectionState) {
          instruments.offsetsSuccessfullyCommitted.get should be(defaultNumberOfEnvelopes)
        }
        eventually {
          instruments.startedInvocations.get should be(1)
          instruments.stoppedInvocations.get should be(1)
          instruments.failureInvocations.get should be(0)
        }
      }
    }
    " in `at-least-once` with flowHandler" must {
      "report nothing in happy scenarios" in {
        val tt =
          new TelemetryTester(AtLeastOnce(), FlowHandlerStrategy[Envelope](TestHandlers.flow))

        runInternal(tt.projectionState) {
          instruments.offsetsSuccessfullyCommitted.get should be(defaultNumberOfEnvelopes)
        }
        eventually {
          instruments.startedInvocations.get should be(1)
          instruments.stoppedInvocations.get should be(1)
          instruments.failureInvocations.get should be(0)
        }
      }
    }

  }
}
