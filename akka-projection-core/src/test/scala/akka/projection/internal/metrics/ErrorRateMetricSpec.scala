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

class ErrorRateMetricSpec extends InternalProjectionStateMetricsSpec {

  "A metric reporting event handler errors" must {
    // at-least-once
    " in `at-least-once` with singleHandler" must {
      "report nothing in happy scenarios" in {
        val tt: TelemetryTester =
          new TelemetryTester(AtLeastOnce(), SingleHandlerStrategy(Handlers.single))
        runInternal(tt.projectionState) {
          detectNoError()
        }
      }
      "report errors in flaky handlers" in {
        val single = Handlers.singleWithFailure(0.1f)
        val tt = new TelemetryTester(
          AtLeastOnce(recoveryStrategy = Some(HandlerRecoveryStrategy.retryAndFail(maxRetries, 30.millis))),
          SingleHandlerStrategy(single))

        runInternal(tt.projectionState) {
          detectSomeErrors()
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
        val grouped = Handlers.groupedWithFailures(0.2f)
        val tt = new TelemetryTester(
          AtLeastOnce(recoveryStrategy = Some(HandlerRecoveryStrategy.retryAndFail(maxRetries, 30.millis))),
          GroupedHandlerStrategy(grouped))

        runInternal(tt.projectionState) {
          detectSomeErrors()
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
        val flow = Handlers.flowWithFailure(0.2f)
        val tt = new TelemetryTester(AtLeastOnce(), FlowHandlerStrategy[Envelope](flow))
        runInternal(tt.projectionState) {
          detectSomeErrors()
        }
      }
    }

    // exactly-once
    " in `exactly-once` with singleHandler" must {
      "report nothing in happy scenarios" in {
        val tt = new TelemetryTester(ExactlyOnce(), SingleHandlerStrategy(Handlers.single))

        runInternal(tt.projectionState) {
          detectNoError()
        }
      }
      "report errors in flaky handlers" in {
        val single = Handlers.singleWithFailure(0.2f)
        val tt = new TelemetryTester(
          ExactlyOnce(recoveryStrategy = Some(HandlerRecoveryStrategy.retryAndFail(maxRetries, 30.millis))),
          SingleHandlerStrategy(single))

        runInternal(tt.projectionState) {
          detectSomeErrors()
        }
      }
    }
    " in `exactly-once` with groupedHandler" must {
      "report nothing in happy scenarios" in {
        val grouped = Handlers.grouped
        val groupHandler = GroupedHandlerStrategy(grouped)
        val tt = new TelemetryTester(ExactlyOnce(), groupHandler)

        runInternal(tt.projectionState) {
          detectNoError()
        }
      }
      "report errors in flaky handlers" in {
        val groupedWithFailures = Handlers.groupedWithFailures(0.2f)
        val tt = new TelemetryTester(
          ExactlyOnce(recoveryStrategy = Some(HandlerRecoveryStrategy.retryAndFail(maxRetries, 30.millis))),
          GroupedHandlerStrategy(groupedWithFailures))

        runInternal(tt.projectionState) {
          detectSomeErrors()
        }
      }
    }

    // at-most-once
    " in `at-most-once` with singleHandler" must {
      "report nothing in happy scenarios" in {
        val tt = new TelemetryTester(ExactlyOnce(), SingleHandlerStrategy(Handlers.single))

        runInternal(tt.projectionState) {
          detectNoError()
        }
      }
      "report nothing in happy scenarios once in case of failure" in {
        val single = Handlers.singleWithFailure(0.2f)
        val tt = new TelemetryTester(
          AtMostOnce(recoveryStrategy = Some(HandlerRecoveryStrategy.skip)),
          SingleHandlerStrategy(single))

        runInternal(tt.projectionState) {
          detectSomeErrors()
        }
      }
    }

  }

  val instruments = InMemInstruments

  def detectNoError(): Any = {
    instruments.offsetsSuccessfullyCommitted.get should be >= 6
    instruments.errorInvocations.get should be(0)
  }

  def detectSomeErrors(): Any = {
    instruments.errorInvocations.get should be > (0)
    if (instruments.lastErrorThrowable.get() != null)
      instruments.lastErrorThrowable.get().getMessage should be("Oh, no! Handler errored.")
  }

}
