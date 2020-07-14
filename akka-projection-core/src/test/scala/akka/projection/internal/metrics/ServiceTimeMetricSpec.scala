/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal.metrics

import scala.concurrent.duration._

import akka.projection.HandlerRecoveryStrategy
import akka.projection.ProjectionId
import akka.projection.internal.AtLeastOnce
import akka.projection.internal.AtMostOnce
import akka.projection.internal.ExactlyOnce
import akka.projection.internal.FlowHandlerStrategy
import akka.projection.internal.GroupedHandlerStrategy
import akka.projection.internal.SingleHandlerStrategy
import akka.projection.internal.metrics.tools.InMemInstrumentsRegistry
import akka.projection.internal.metrics.tools.InternalProjectionStateMetricsSpec
import akka.projection.internal.metrics.tools.InternalProjectionStateMetricsSpec._
import akka.projection.internal.metrics.tools.TestHandlers

sealed abstract class ServiceTimeAndProcessingCountMetricSpec extends InternalProjectionStateMetricsSpec {
  implicit var projectionId: ProjectionId = null

  before {
    projectionId = genRandomProjectionId()
  }

  def instruments(implicit projectionId: ProjectionId) = InMemInstrumentsRegistry(system).forId(projectionId)
  val defaultNumberOfEnvelopes = 6

}

class ServiceTimeAndProcessingCountMetricAtLeastOnceSpec extends ServiceTimeAndProcessingCountMetricSpec {

  "A metric reporting ServiceTime" must {
    " in `at-least-once` with singleHandler" must {
      "reports measures for all envelopes (without afterEnvelops optimization)" in {
        val single = TestHandlers.single
        val tt =
          new TelemetryTester(AtLeastOnce(afterEnvelopes = Some(1)), SingleHandlerStrategy(single))

        runInternal(tt.projectionState) {
          instruments.afterProcessInvocations.get should be(defaultNumberOfEnvelopes)
          instruments.lastServiceTimeInNanos.get() should be > (0L)
        }
      }
      "reports measures for all envelopes (with afterEnvelops optimization)" in {
        val tt =
          new TelemetryTester(AtLeastOnce(afterEnvelopes = Some(3)), SingleHandlerStrategy(TestHandlers.single))

        runInternal(tt.projectionState) {
          // afterProcess invocations happen per envelope (not in a groupWithin!)
          instruments.afterProcessInvocations.get should be(defaultNumberOfEnvelopes)
          instruments.lastServiceTimeInNanos.get() should be > (0L)

        }
      }
      "reports measures for all envelopes (multiple times when there are failures) " in {
        val single = TestHandlers.singleWithErrors(3, 5)
        val numberOfEnvelopes = 6
        val tt = new TelemetryTester(
          AtLeastOnce(
            afterEnvelopes = Some(numberOfEnvelopes - 1),
            recoveryStrategy = Some(HandlerRecoveryStrategy.fail)),
          SingleHandlerStrategy(single))
        runInternal(tt.projectionState) {
          instruments.errorInvocations.get should be(2)
          // There's be 12 invocations to afterProcessInvocations because
          // the Errors in `singleWithErrors(3, 5)` cause the following invocations:
          //  - batch with [1,2,3,4,5] runs [1,2] and fails [3] (error(3) is dropped from the error stack)
          //  - batch with [1,2,3,4,5] runs [1,2,3,4] and fails [5] (error(5) is dropped from the error stack)
          //  - batch with [1,2,3,4,5] runs [1,2,3,4,5] and commits offset [5]
          //  - batch with [6] runs [6] and commits offset [6]
          instruments.afterProcessInvocations.get should be(12)
        }
      }
    }
    " in `at-least-once` with groupedHandler" must {
      "report measures for each envelope (without afterEnvelops optimization)" in {
        val tt = new TelemetryTester(
          AtLeastOnce(afterEnvelopes = Some(1)),
          GroupedHandlerStrategy(TestHandlers.grouped, afterEnvelopes = Some(2), orAfterDuration = Some(30.millis)))

        runInternal(tt.projectionState) {
          instruments.afterProcessInvocations.get should be(defaultNumberOfEnvelopes)
        }
      }

      "report measures per envelope (with afterEnvelops optimization)" in {
        val tt = new TelemetryTester(
          AtLeastOnce(afterEnvelopes = Some(3)),
          GroupedHandlerStrategy(TestHandlers.grouped, afterEnvelopes = Some(2), orAfterDuration = Some(30.millis)))

        runInternal(tt.projectionState) {
          // even when grouping, there's 6 time measures reported
          instruments.afterProcessInvocations.get should be(defaultNumberOfEnvelopes)
        }
      }

      "report multiple measures per envelope in case of failure (recovery == fail)" in {
        val grouped = TestHandlers.groupedWithErrors(3, 5, 6)
        // magic numbers to have at least a a batch of 4 groups of 3 envelopes (and one extra envelope)
        // 13 = 12+1 = (4*3)+1
        val numberOfEnvelopes = 13
        val tt = new TelemetryTester(
          AtLeastOnce(afterEnvelopes = Some(4), recoveryStrategy = Some(HandlerRecoveryStrategy.fail)),
          GroupedHandlerStrategy(grouped, afterEnvelopes = Some(3), orAfterDuration = Some(30.millis)),
          numberOfEnvelopes = numberOfEnvelopes)

        runInternal(tt.projectionState) {
          instruments.errorInvocations.get should be(3)
          // The number of invocations is 19 because:
          //  - a batch of 4 groups of 3 items is processed:
          //      [(123)(456)(789)(...)] but '3' errors and nothing is reported (then 3 is removed from error stack)
          //      report 0
          //  - a batch of 4 groups of 3 items is processed:
          //      [(123)(456)(789)(...)] but '5' errors and (123) are reported (then 5 is removed from error stack)
          //      report 3
          //  - a batch of 4 groups of 3 items is processed:
          //      [(123)(456)(789)(...)] but '6' errors and (123) are reported  (then 6 is removed from error stack)
          //      report 3
          //  - a batch of 4 groups of 3 items is processed:
          //      [(123)(456)(789)(...)] and all are reported
          //      report 12
          //  - a final batch of 1 group of 1 item is processed:
          //      [(13)] and all are reported
          //      report 1
          instruments.afterProcessInvocations.get should be(0 + 3 + 3 + 12 + 1)
        }
      }

      "report multiple measures per envelope in case of failure (recovery == retryAndSkip)" in {
        // Envelopes 3 and 7 will error twice so they must be skipped. Note that `retries = 1`
        // means there's 1 retry _after_ an initial error so an envelope must error
        // `retries+1` times to be skipped.
        val grouped = TestHandlers.groupedWithErrors(3, 3, 6, 7, 7)
        // magic number to have at least a complete batch of 2 groups of 2 envelopes after the last error (offset==7)
        val numberOfEnvelopes = 13
        val tt = new TelemetryTester(
          AtLeastOnce(
            afterEnvelopes = Some(2),
            recoveryStrategy = Some(HandlerRecoveryStrategy.retryAndSkip(retries = 1, 10.millis))),
          GroupedHandlerStrategy(grouped, afterEnvelopes = Some(2), orAfterDuration = Some(50.millis)),
          numberOfEnvelopes = numberOfEnvelopes)

        runInternal(tt.projectionState) {
          // The number of invocations is 19 because:
          //  - a batch of 2 groups of 2 items is processed:
          //      [(1 2)(3 4)] but '3' errors, then it's retried, it errors again and it's skipped.
          //      report 2 envelopes (1 2)
          //  - a batch of 2 groups of 2 items is processed:
          //      [(5 6)(7 8)] but '6' errors, reties and succeeds, then 7 errors twice and is skipped
          //      report 2 envelopes (5 6)
          //  - a batch of 2 groups of 2 items is processed:
          //      [(9 10)(11 12)]
          //      report 4
          //  - a batch of 1 groups of 1 items is processed:
          //      [(13)]
          //      report 1
          instruments.afterProcessInvocations.get should be(2 + 2 + 4 + 1)
        }
      }

    }
    " in `at-least-once` with flowHandler" must {
      "report a measure per envelope" in {
        val tt =
          new TelemetryTester(AtLeastOnce(afterEnvelopes = Some(3)), FlowHandlerStrategy[Envelope](TestHandlers.flow))

        runInternal(tt.projectionState) {
          instruments.afterProcessInvocations.get should be(defaultNumberOfEnvelopes)
        }
      }
      "report multiple measures per envelope in case of failure" in {
        val flow = TestHandlers.flowWithErrors(2, 5, 6)
        val tt =
          new TelemetryTester(AtLeastOnce(afterEnvelopes = Some(2)), FlowHandlerStrategy[Envelope](flow))
        runInternal(tt.projectionState) {
          instruments.afterProcessInvocations.get should be(10)
        }
      }
    }

  }

}

class ServiceTimeAndProcessingCountMetricExactlyOnceSpec extends ServiceTimeAndProcessingCountMetricSpec {

  "A metric reporting ServiceTime" must {

    // exactly-once
    " in `exactly-once` with singleHandler" must {
      "report only one measure per envelope" in {
        val tt =
          new TelemetryTester(ExactlyOnce(), SingleHandlerStrategy(TestHandlers.single))

        runInternal(tt.projectionState) {
          instruments.afterProcessInvocations.get should be(defaultNumberOfEnvelopes)
        }
      }
      "report only one measure per envelope even in case of failure" in {
        val single = TestHandlers.singleWithErrors(2, 2, 2, 3, 3, 3, 5)
        val tt = new TelemetryTester(
          // using retryAndFail to try to get all message through
          ExactlyOnce(recoveryStrategy = Some(HandlerRecoveryStrategy.fail)),
          SingleHandlerStrategy(single))

        runInternal(tt.projectionState) {
          // even in case of failures, the number of reported time measures is equal to the number of successes
          instruments.lastErrorThrowable.get should not be null
          instruments.errorInvocations.get should be(7)
          instruments.afterProcessInvocations.get should be(defaultNumberOfEnvelopes)
          instruments.lastServiceTimeInNanos.get should be > 0L
        }
      }
    }
    " in `exactly-once` with groupedHandler" must {
      "report only one measure per envelope" in {
        val grouped = TestHandlers.grouped
        val groupHandler = GroupedHandlerStrategy(grouped, afterEnvelopes = Some(2), orAfterDuration = Some(30.millis))
        val tt =
          new TelemetryTester(ExactlyOnce(), groupHandler)
        runInternal(tt.projectionState) {
          instruments.afterProcessInvocations.get should be(defaultNumberOfEnvelopes)
        }
      }

      "report only one measure per envelope even in case of failure (recovery is fail)" in {
        val groupedWithFailures = TestHandlers.groupedWithErrors(5)
        val tt = new TelemetryTester(
          ExactlyOnce(recoveryStrategy = Some(HandlerRecoveryStrategy.fail)),
          GroupedHandlerStrategy(groupedWithFailures, afterEnvelopes = Some(2), orAfterDuration = Some(10.millis)))
        runInternal(tt.projectionState) {
          instruments.lastErrorThrowable.get should not be null
          instruments.afterProcessInvocations.get should be(defaultNumberOfEnvelopes)
        }
      }

      "report only one measure per envelope even in case of failure (recovery is skip)" in {
        val groupedWithFailures = TestHandlers.groupedWithErrors(1, 7)
        val numberOfEnvelopes = 11
        val tt = new TelemetryTester(
          ExactlyOnce(recoveryStrategy = Some(HandlerRecoveryStrategy.skip)),
          GroupedHandlerStrategy(groupedWithFailures, afterEnvelopes = Some(2), orAfterDuration = Some(10.millis)),
          numberOfEnvelopes)
        runInternal(tt.projectionState) {
          instruments.lastErrorThrowable.get should not be null
          // [(12)] -> 0
          // [(34)] -> 2
          // [(56)] -> 2
          // [(78)] -> 0
          // [(9 10)] -> 2
          // [(11)] -> 1
          instruments.afterProcessInvocations.get should be(0 + 2 + 2 + 0 + 2 + 1)
        }
      }

    }

  }

}

class ServiceTimeAndProcessingCountMetricAtMostOnceSpec extends ServiceTimeAndProcessingCountMetricSpec {

  "A metric reporting ServiceTime" must {

    // at-most-once
    " in `at-most-once` with singleHandler" must {
      "report measures" in {
        val tt =
          new TelemetryTester(ExactlyOnce(), SingleHandlerStrategy(TestHandlers.single))
        runInternal(tt.projectionState) {
          instruments.afterProcessInvocations.get should be(defaultNumberOfEnvelopes)
          instruments.lastServiceTimeInNanos.get should be > 0L
        }
      }
      "report measures if envelopes were processed in case of failure" in {
        val single = TestHandlers.singleWithErrors(4, 5, 6)
        val numberOfEnvelopes = 100
        val tt = new TelemetryTester(
          AtMostOnce(recoveryStrategy = Some(HandlerRecoveryStrategy.fail)),
          SingleHandlerStrategy(single),
          numberOfEnvelopes)

        runInternal(tt.projectionState) {
          instruments.lastErrorThrowable.get should not be null
          instruments.afterProcessInvocations.get should be(97)
          instruments.lastServiceTimeInNanos.get should be > 0L
        }
      }
    }

  }

}
