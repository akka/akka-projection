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

class OffsetCommittedCounterMetricSpec extends InternalProjectionStateMetricsSpec {

  val instruments = InMemInstruments

  "A metric counting offsets committed" must {
    // at-least-once
    " in `at-least-once` with singleHandler" must {
      "count offsets (without afterEnvelops optimization)" in {
        val numberOfEnvelopes = 6
        val single = Handlers.single
        val tt = new TelemetryTester(
          AtLeastOnce(afterEnvelopes = Some(1)),
          SingleHandlerStrategy(single),
          numberOfEnvelopes = numberOfEnvelopes)

        runInternal(tt.projectionState) {
          withClue("check - all values were concatenated") {
            single.concatStr shouldBe "a|b|c|d|e|f"
          }
          withClue("the success counter reflects all events as processed") {
            instruments.offsetsSuccessfullyCommitted.get should be(numberOfEnvelopes)
            instruments.onOffsetStoredInvocations.get should be(numberOfEnvelopes)
          }
        }
      }
      "count offsets (with afterEnvelops optimization)" in {
        val batchSize = 3
        val numberOfEnvelopes = 6
        val tt =
          new TelemetryTester(
            AtLeastOnce(afterEnvelopes = Some(batchSize)),
            SingleHandlerStrategy(Handlers.single),
            numberOfEnvelopes = numberOfEnvelopes)

        runInternal(tt.projectionState) {
          withClue("the success counter reflects all events as processed") {
            instruments.offsetsSuccessfullyCommitted.get should be(numberOfEnvelopes)
            instruments.onOffsetStoredInvocations.get should be(2)
          }
        }
      }
      "count offsets only once in case of failure" in {
        val single = Handlers.singleWithFailure(0.5f)
        val tt = new TelemetryTester(
          AtLeastOnce(
            afterEnvelopes = Some(1),
            // using retryAndFail to try to get all message through
            recoveryStrategy = Some(HandlerRecoveryStrategy.retryAndFail(maxRetries, 30.millis))),
          SingleHandlerStrategy(single))

        runInternal(tt.projectionState) {
          withClue("the success counter reflects all events as processed") {
            instruments.offsetsSuccessfullyCommitted.get should be(6)
            instruments.onOffsetStoredInvocations.get should be(6)
          }
        }
      }
    }
    " in `at-least-once` with groupedHandler" must {
      "count offsets (without afterEnvelops optimization)" in {
        val tt = new TelemetryTester(
          AtLeastOnce(afterEnvelopes = Some(1)),
          GroupedHandlerStrategy(Handlers.grouped, afterEnvelopes = Some(2), orAfterDuration = Some(500.millis)))

        runInternal(tt.projectionState) {
          withClue("the success counter reflects all events as processed") {
            instruments.offsetsSuccessfullyCommitted.get should be(6)
            instruments.onOffsetStoredInvocations.get should be(3)
          }
        }
      }
      "count offsets (with afterEnvelops optimization)" in {
        val tt = new TelemetryTester(
          AtLeastOnce(afterEnvelopes = Some(3)),
          GroupedHandlerStrategy(Handlers.grouped, afterEnvelopes = Some(2), orAfterDuration = Some(500.millis)))

        runInternal(tt.projectionState) {
          withClue("the success counter reflects all events as processed") {
            instruments.offsetsSuccessfullyCommitted.get should be(6)
            instruments.onOffsetStoredInvocations.get should be(1)
          }
        }
      }
      "count envelopes only once in case of failure" in {
        val grouped = Handlers.groupedWithFailures(0.5f)
        val tt = new TelemetryTester(
          AtLeastOnce(
            afterEnvelopes = Some(2),
            recoveryStrategy = Some(HandlerRecoveryStrategy.retryAndFail(maxRetries, 30.millis))),
          GroupedHandlerStrategy(grouped, afterEnvelopes = Some(3), orAfterDuration = Some(500.millis)))

        runInternal(tt.projectionState) {
          withClue("the success counter reflects all events as processed") {
            instruments.offsetsSuccessfullyCommitted.get should be(6)
            instruments.onOffsetStoredInvocations.get should be(1)
          }
        }
      }
    }
    " in `at-least-once` with flowHandler" must {
      "count offsets" in {
        val tt =
          new TelemetryTester(AtLeastOnce(afterEnvelopes = Some(3)), FlowHandlerStrategy[Envelope](Handlers.flow))

        runInternal(tt.projectionState) {
          withClue("the success counter reflects all events as processed") {
            instruments.offsetsSuccessfullyCommitted.get should be(6)
            instruments.onOffsetStoredInvocations.get should be(2)
          }
        }
      }
      "count offsets only once in case of failure" in {
        val flow = Handlers.flowWithFailureAndRetries(0.8f, maxRetries)
        val tt =
          new TelemetryTester(AtLeastOnce(afterEnvelopes = Some(2)), FlowHandlerStrategy[Envelope](flow))
        runInternal(tt.projectionState) {
          withClue("the success counter reflects all events as processed") {
            instruments.offsetsSuccessfullyCommitted.get should be(6)
            instruments.onOffsetStoredInvocations.get should be(3)
          }
        }
      }
    }

    // exactly-once
    " in `exactly-once` with singleHandler" must {
      "count offsets" in {
        val tt =
          new TelemetryTester(ExactlyOnce(), SingleHandlerStrategy(Handlers.single))

        runInternal(tt.projectionState) {
          withClue("the success counter reflects all events as processed") {
            instruments.offsetsSuccessfullyCommitted.get should be(6)
            instruments.onOffsetStoredInvocations.get should be(6)
          }
        }
      }
      "count offsets only once in case of failure" in {
        val single = Handlers.singleWithFailure(0.5f)
        val tt = new TelemetryTester(
          // using retryAndFail to try to get all message through
          ExactlyOnce(recoveryStrategy = Some(HandlerRecoveryStrategy.retryAndFail(maxRetries, 30.millis))),
          SingleHandlerStrategy(single))

        runInternal(tt.projectionState) {
          withClue("the success counter reflects all events as processed") {
            instruments.offsetsSuccessfullyCommitted.get should be(6)
          }
        }
      }
    }
    " in `exactly-once` with groupedHandler" must {
      "count offsets" in {
        val grouped = Handlers.grouped
        val groupHandler = GroupedHandlerStrategy(grouped, afterEnvelopes = Some(2))
        val tt =
          new TelemetryTester(ExactlyOnce(), groupHandler)

        runInternal(tt.projectionState) {
          withClue("the success counter reflects all events as processed") {
            instruments.offsetsSuccessfullyCommitted.get should be(6)
            instruments.onOffsetStoredInvocations.get should be(3)
          }
        }
      }
      "count offsets only once in case of failure" in {
        val groupedWithFailures = Handlers.groupedWithFailures(0.5f)
        val tt = new TelemetryTester(
          ExactlyOnce(recoveryStrategy = Some(HandlerRecoveryStrategy.retryAndFail(maxRetries, 30.millis))),
          GroupedHandlerStrategy(groupedWithFailures, afterEnvelopes = Some(2)))

        runInternal(tt.projectionState) {
          withClue("the success counter reflects all events as processed") {
            instruments.offsetsSuccessfullyCommitted.get should be(6)
          }
        }
      }
    }
    " in `exactly-once` with flowHandler count offsets (UNSUPPORTED)" ignore {}

    // at-most-once
    " in `at-most-once` with singleHandler" must {
      "count offsets" in {
        val tt =
          new TelemetryTester(ExactlyOnce(), SingleHandlerStrategy(Handlers.single))

        runInternal(tt.projectionState) {
          withClue("the success counter reflects all events as processed") {
            instruments.offsetsSuccessfullyCommitted.get should be(6)
            instruments.onOffsetStoredInvocations.get should be(6)
          }
        }
      }
      "count offsets once in case of failure" in {
        val single = Handlers.singleWithFailure(0.5f)
        val tt = new TelemetryTester(
          // using retryAndFail to try to get all message through
          AtMostOnce(recoveryStrategy = Some(HandlerRecoveryStrategy.skip)),
          SingleHandlerStrategy(single))

        runInternal(tt.projectionState) {
          withClue("the success counter reflects all events as processed") {
            instruments.offsetsSuccessfullyCommitted.get should be(6)
          }
        }
      }
    }
    " in `at-most-once` with groupedHandler count offsets (UNSUPPORTED)" ignore {}
    " in `at-most-once` with flowHandler count offsets (UNSUPPORTED)" ignore {}

  }

}
