package akka.projection.internal.metrics

import scala.concurrent.duration._

import akka.projection.HandlerRecoveryStrategy
import akka.projection.internal.AtLeastOnce
import akka.projection.internal.AtMostOnce
import akka.projection.internal.ExactlyOnce
import akka.projection.internal.FlowHandlerStrategy
import akka.projection.internal.GroupedHandlerStrategy
import akka.projection.internal.SingleHandlerStrategy
import akka.projection.internal.metrics.InternalProjectionStateMetricsSpec._

class ServiceTimeMetricSpec extends InternalProjectionStateMetricsSpec {

  val instruments = InMemInstruments

  val defaultNumberOfEnvelopes = 6

  "A metric reporting ServiceTime committed" must {
    // at-least-once
    " in `at-least-once` with singleHandler" must {
      "reports measures for all envelopes (without afterEnvelops optimization)" in {
        val single = Handlers.single
        val tt = new TelemetryTester(AtLeastOnce(afterEnvelopes = Some(1)), SingleHandlerStrategy(single))

        runInternal(tt.projectionState) {
          withClue("the success counter reflects all events as processed") {
            instruments.afterProcessInvocations.get should be(defaultNumberOfEnvelopes)
            instruments.lastServiceTimeInNanos.get() should be > (0L)
          }
        }
      }
      "reports measures for all envelopes (with afterEnvelops optimization)" in {
        val tt =
          new TelemetryTester(AtLeastOnce(afterEnvelopes = Some(3)), SingleHandlerStrategy(Handlers.single))

        runInternal(tt.projectionState) {
          withClue("the success counter reflects all events as processed") {
            // afterProcess invocations happen per envelope (not in a groupWithin!)
            instruments.afterProcessInvocations.get should be(defaultNumberOfEnvelopes)
            instruments.lastServiceTimeInNanos.get() should be > (0L)
          }
        }
      }
      "reports measures for all envelopes (multiple times when there are failures) " in {
        val single = Handlers.singleWithFailure(0.5f)
        val numberOfEnvelopes = 6
        val tt = new TelemetryTester(
          AtLeastOnce(
            // bigger batches advance the error counter faster (speeding up the test)
            afterEnvelopes = Some(numberOfEnvelopes - 1),
            recoveryStrategy = Some(HandlerRecoveryStrategy.fail)),
          SingleHandlerStrategy(single))
        runInternal(tt.projectionState) {
          instruments.errorInvocations.get should be > 0
          instruments.afterProcessInvocations.get should be > (numberOfEnvelopes)
        }
      }
    }
    " in `at-least-once` with groupedHandler" must {
      "report measures for each envelope (without afterEnvelops optimization)" in {
        val tt = new TelemetryTester(
          AtLeastOnce(afterEnvelopes = Some(1)),
          GroupedHandlerStrategy(Handlers.grouped, afterEnvelopes = Some(2), orAfterDuration = Some(30.millis)))

        runInternal(tt.projectionState) {
          instruments.afterProcessInvocations.get should be(defaultNumberOfEnvelopes)
        }
      }
      "report measures per envelope (with afterEnvelops optimization)" in {
        val tt = new TelemetryTester(
          AtLeastOnce(afterEnvelopes = Some(3)),
          GroupedHandlerStrategy(Handlers.grouped, afterEnvelopes = Some(2), orAfterDuration = Some(30.millis)))

        runInternal(tt.projectionState) {
          // even when grouping, there's 6 time measures reported
          instruments.afterProcessInvocations.get should be(defaultNumberOfEnvelopes)
        }
      }

      "report measures per envelope in case of failure" in {
        val grouped = Handlers.groupedWithFailures(0.6f)
        // 13 = 12+1 = (4*3)+1 // cooked numbers to quickly fail bigger batches speeding up the test
        val numberOfEnvelopes = 13
        val tt = new TelemetryTester(
          AtLeastOnce(afterEnvelopes = Some(4), recoveryStrategy = Some(HandlerRecoveryStrategy.fail)),
          GroupedHandlerStrategy(grouped, afterEnvelopes = Some(3), orAfterDuration = Some(30.millis)),
          numberOfEnvelopes = numberOfEnvelopes)

        runInternal(tt.projectionState) {
          instruments.errorInvocations.get should be > 0
          instruments.afterProcessInvocations.get should be > (numberOfEnvelopes)
        }
      }
    }
    " in `at-least-once` with flowHandler" must {
      "report a measure per envelope" in {
        val tt =
          new TelemetryTester(AtLeastOnce(afterEnvelopes = Some(3)), FlowHandlerStrategy[Envelope](Handlers.flow))

        runInternal(tt.projectionState) {
          instruments.afterProcessInvocations.get should be(defaultNumberOfEnvelopes)
        }
      }
      "report multiple measures per envelope in case of failure" in {
        // disable the reties in the user flow so failures propagate and are counted. Then, it's
        // the projection `RestartSource` that's handling the retry.
        val retriesInUserFlow = 0
        val flow = Handlers.flowWithFailureAndRetries(0.3f, retriesInUserFlow)
        val tt =
          new TelemetryTester(AtLeastOnce(afterEnvelopes = Some(2)), FlowHandlerStrategy[Envelope](flow))
        runInternal(tt.projectionState) {
          instruments.afterProcessInvocations.get should be > 6
        }
      }
    }

    // exactly-once
    " in `exactly-once` with singleHandler" must {
      "report only one measure per envelope" in {
        val tt =
          new TelemetryTester(ExactlyOnce(), SingleHandlerStrategy(Handlers.single))

        runInternal(tt.projectionState) {
          instruments.afterProcessInvocations.get should be(defaultNumberOfEnvelopes)
        }
      }
      "report only one measure per envelope even in case of failure" in {
        val single = Handlers.singleWithFailure(0.5f)
        val tt = new TelemetryTester(
          // using retryAndFail to try to get all message through
          ExactlyOnce(recoveryStrategy = Some(HandlerRecoveryStrategy.fail)),
          SingleHandlerStrategy(single))

        runInternal(tt.projectionState) {
          // even in case of failures, the number of reported time measures is equal to the number of successes
          instruments.lastErrorThrowable.get should not be null
          instruments.afterProcessInvocations.get should be(defaultNumberOfEnvelopes)
        }
      }
    }
    " in `exactly-once` with groupedHandler" must {
      "report only one measure per envelope" in {
        val grouped = Handlers.grouped
        val groupHandler = GroupedHandlerStrategy(grouped, afterEnvelopes = Some(2), orAfterDuration = Some(30.millis))
        val tt =
          new TelemetryTester(ExactlyOnce(), groupHandler)
        runInternal(tt.projectionState) {
          withClue("the success counter reflects all events as processed") {
            instruments.afterProcessInvocations.get should be(defaultNumberOfEnvelopes)
          }
        }
      }
      "report only one measure per envelope even in case of failure" in {
        val groupedWithFailures = Handlers.groupedWithFailures(0.5f)
        val tt = new TelemetryTester(
          ExactlyOnce(recoveryStrategy = Some(HandlerRecoveryStrategy.fail)),
          GroupedHandlerStrategy(groupedWithFailures, afterEnvelopes = Some(2), orAfterDuration = Some(30.millis)))
        runInternal(tt.projectionState) {
          withClue("the success counter reflects all events as processed") {
            instruments.lastErrorThrowable.get should not be null
            instruments.afterProcessInvocations.get should be(defaultNumberOfEnvelopes)
          }
        }
      }
    }
    " in `exactly-once` with flowHandler count offsets (UNSUPPORTED)" ignore {}

    // at-most-once
    " in `at-most-once` with singleHandler" must {
      "report measures" in {
        val tt =
          new TelemetryTester(ExactlyOnce(), SingleHandlerStrategy(Handlers.single))
        runInternal(tt.projectionState) {
          instruments.afterProcessInvocations.get should be(6)
        }
      }
      "report measures if envelopes were processed in case of failure" in {
        val single = Handlers.singleWithFailure(0.2f)
        val tt = new TelemetryTester(
          // using retryAndFail to try to get all message through
          AtMostOnce(recoveryStrategy = Some(HandlerRecoveryStrategy.fail)),
          SingleHandlerStrategy(single))

        runInternal(tt.projectionState) {
          instruments.lastErrorThrowable.get should not be null
          instruments.afterProcessInvocations.get should be < defaultNumberOfEnvelopes
          instruments.offsetsSuccessfullyCommitted.get should be(defaultNumberOfEnvelopes)
        }
      }
    }
    " in `at-most-once` with groupedHandler count offsets (UNSUPPORTED)" ignore {}
    " in `at-most-once` with flowHandler count offsets (UNSUPPORTED)" ignore {}

  }

}
