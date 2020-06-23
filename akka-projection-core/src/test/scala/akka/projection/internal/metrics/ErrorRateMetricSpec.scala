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

abstract class ErrorRateMetricSpec extends InternalProjectionStateMetricsSpec {
  val instruments = InMemInstruments
  val defaultNumberOfEnvelopes = 6

  def detectNoError(numberOfEnvelopes: Int = defaultNumberOfEnvelopes): Any = {
    instruments.offsetsSuccessfullyCommitted.get should be(numberOfEnvelopes)
    instruments.errorInvocations.get should be(0)
  }

  def detectSomeErrors(expectedErrorCount: Int): Any = {
    instruments.errorInvocations.get should be(expectedErrorCount)
    if (instruments.lastErrorThrowable.get() != null)
      instruments.lastErrorThrowable.get().getMessage should be("Oh, no! Handler errored.")
  }

}

class ErrorRateMetricAtLeastOnceSpec extends ErrorRateMetricSpec {

  "A metric reporting event handler errors" must {
    // at-least-once
    " in `at-least-once` with singleHandler" must {
      "report nothing in happy scenarios" in {
        val numOfEnvelopes = 20
        val tt: TelemetryTester =
          new TelemetryTester(AtLeastOnce(), SingleHandlerStrategy(Handlers.single), numOfEnvelopes)
        runInternal(tt.projectionState) {
          detectNoError(numOfEnvelopes)
        }
      }
      "report errors in flaky handlers" in {
        val single = Handlers.singleWithErrors(1, 1, 1, 1, 2, 2, 3, 4, 5)
        val tt = new TelemetryTester(
          AtLeastOnce(recoveryStrategy = Some(HandlerRecoveryStrategy.retryAndFail(maxRetries, 30.millis))),
          SingleHandlerStrategy(single))

        runInternal(tt.projectionState) {
          detectSomeErrors(9)
        }
      }
    }
    " in `at-least-once` with groupedHandler" must {
      "report nothing in happy scenarios" in {
        val tt = new TelemetryTester(AtLeastOnce(), GroupedHandlerStrategy(Handlers.grouped))

        runInternal(tt.projectionState) {
          detectNoError()
        }
      }
      "report errors in flaky handlers" in {
        val grouped = Handlers.groupedWithErrors(2, 3, 3, 5)
        val tt = new TelemetryTester(
          AtLeastOnce(recoveryStrategy = Some(HandlerRecoveryStrategy.retryAndFail(maxRetries, 30.millis))),
          GroupedHandlerStrategy(grouped))

        runInternal(tt.projectionState) {
          detectSomeErrors(4)
        }
      }
    }
    " in `at-least-once` with flowHandler" must {
      "report nothing in happy scenarios" in {
        val tt =
          new TelemetryTester(AtLeastOnce(), FlowHandlerStrategy[Envelope](Handlers.flow))

        runInternal(tt.projectionState) {
          detectNoError()
        }
      }
      "report errors in flaky handlers" in {
        val flow = Handlers.flowWithErrors(2, 3, 6)
        val tt = new TelemetryTester(AtLeastOnce(), FlowHandlerStrategy[Envelope](flow))
        runInternal(tt.projectionState) {
          detectSomeErrors(3)
        }
      }
    }

  }
}

class ErrorRateMetricExactlyOnceSpec extends ErrorRateMetricSpec {

  "A metric reporting event handler errors" must {

    // exactly-once
    " in `exactly-once` with singleHandler" must {
      "report nothing in happy scenarios" in {
        val tt = new TelemetryTester(ExactlyOnce(), SingleHandlerStrategy(Handlers.single))

        runInternal(tt.projectionState) {
          detectNoError()
        }
      }
      "report errors in flaky handlers" in {
        val single = Handlers.singleWithErrors(2, 3, 4, 4, 4, 4, 4, 4, 4, 5, 6, 6)
        val tt = new TelemetryTester(
          ExactlyOnce(recoveryStrategy = Some(HandlerRecoveryStrategy.retryAndFail(maxRetries, 30.millis))),
          SingleHandlerStrategy(single))

        runInternal(tt.projectionState) {
          detectSomeErrors(12)
        }
      }
    }
    " in `exactly-once` with groupedHandler" must {
      "report nothing in happy scenarios" in {
        val grouped = Handlers.grouped
        val groupHandler: GroupedHandlerStrategy[Envelope] = GroupedHandlerStrategy(grouped)
        val tt = new TelemetryTester(ExactlyOnce(), groupHandler)

        runInternal(tt.projectionState) {
          detectNoError()
        }
      }
      "report errors in flaky handlers" in {
        val groupedWithFailures = Handlers.groupedWithErrors(1, 2, 5)
        val tt = new TelemetryTester(
          ExactlyOnce(recoveryStrategy = Some(HandlerRecoveryStrategy.retryAndFail(maxRetries, 30.millis))),
          GroupedHandlerStrategy(groupedWithFailures))

        runInternal(tt.projectionState) {
          detectSomeErrors(3)
        }
      }
    }

  }
}

class ErrorRateMetricAtMostOnceSpec extends ErrorRateMetricSpec {

  "A metric reporting event handler errors" must {

    // at-most-once
    " in `at-most-once` with singleHandler" must {
      "report nothing in happy scenarios" in {
        val tt = new TelemetryTester(ExactlyOnce(), SingleHandlerStrategy(Handlers.single))

        runInternal(tt.projectionState) {
          detectNoError()
        }
      }
      "report nothing in case of failure" in {
        val single = Handlers.singleWithErrors(1, 2, 4, 6)
        val tt = new TelemetryTester(
          AtMostOnce(recoveryStrategy = Some(HandlerRecoveryStrategy.skip)),
          SingleHandlerStrategy(single))

        runInternal(tt.projectionState) {
          detectSomeErrors(4)
        }
      }
    }

  }

}
