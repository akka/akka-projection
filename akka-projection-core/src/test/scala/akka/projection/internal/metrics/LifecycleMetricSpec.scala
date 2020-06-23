/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal.metrics

import akka.projection.HandlerRecoveryStrategy
import akka.projection.internal.AtLeastOnce
import akka.projection.internal.FlowHandlerStrategy
import akka.projection.internal.GroupedHandlerStrategy
import akka.projection.internal.SingleHandlerStrategy
import akka.projection.internal.metrics.InternalProjectionStateMetricsSpec._

class LifecycleMetricSpec extends InternalProjectionStateMetricsSpec {
  val instruments = InMemInstruments
  val defaultNumberOfEnvelopes = 6

  "A metric reporting projection lifecycle metrics" must {
    // at-least-once
    " in `at-least-once` with singleHandler" must {
      "count a start and a stop" in {
        val numOfEnvelopes = 20
        val tt: TelemetryTester =
          new TelemetryTester(AtLeastOnce(), SingleHandlerStrategy(Handlers.single), numOfEnvelopes)
        runInternal(tt.projectionState) {
          instruments.offsetsSuccessfullyCommitted.get should be(numOfEnvelopes)
        }
        instruments.startedInvocations.get should be(1)
        instruments.stoppedInvocations.get should be(1)
      }
      "count projection failures" in {
        val single = Handlers.singleWithErrors(1, 1, 1, 1, 2, 2, 3, 4, 5)
        val tt = new TelemetryTester(
          AtLeastOnce(recoveryStrategy = Some(HandlerRecoveryStrategy.fail)),
          SingleHandlerStrategy(single))

        runInternal(tt.projectionState) {
          instruments.offsetsSuccessfullyCommitted.get should be(defaultNumberOfEnvelopes)
        }
        instruments.startedInvocations.get should be(10)
        instruments.stoppedInvocations.get should be(1)
        instruments.failureInvocations.get should be(9)
      }
    }

    " in `at-least-once` with groupedHandler" must {
      "report nothing in happy scenarios" in {
        val tt = new TelemetryTester(AtLeastOnce(), GroupedHandlerStrategy(Handlers.grouped))

        runInternal(tt.projectionState) {
          instruments.offsetsSuccessfullyCommitted.get should be(defaultNumberOfEnvelopes)
        }
        instruments.startedInvocations.get should be(1)
        instruments.stoppedInvocations.get should be(1)
        instruments.failureInvocations.get should be(0)
      }
    }
    " in `at-least-once` with flowHandler" must {
      "report nothing in happy scenarios" in {
        val tt =
          new TelemetryTester(AtLeastOnce(), FlowHandlerStrategy[Envelope](Handlers.flow))

        runInternal(tt.projectionState) {
          instruments.offsetsSuccessfullyCommitted.get should be(defaultNumberOfEnvelopes)
        }
        instruments.startedInvocations.get should be(1)
        instruments.stoppedInvocations.get should be(1)
        instruments.failureInvocations.get should be(0)
      }
    }

  }
}
