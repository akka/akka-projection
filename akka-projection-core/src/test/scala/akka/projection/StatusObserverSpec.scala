/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

import scala.concurrent.duration._

import akka.projection.internal.AtLeastOnce
import akka.projection.internal.AtMostOnce
import akka.projection.internal.ExactlyOnce
import akka.projection.internal.FlowHandlerStrategy
import akka.projection.internal.GroupedHandlerStrategy
import akka.projection.internal.SingleHandlerStrategy
import akka.projection.internal.metrics.tools.InMemInstrumentsRegistry
import akka.projection.internal.metrics.tools.InternalProjectionStateMetricsSpec
import akka.projection.internal.metrics.tools.InternalProjectionStateMetricsSpec.Envelope
import akka.projection.internal.metrics.tools.InternalProjectionStateMetricsSpec.TelemetryTester
import akka.projection.internal.metrics.tools.TelemetryException
import akka.projection.internal.metrics.tools.TestHandlers

// TODO: use a simpler InternalProjectionStateMetricsSpec (without metrics (?), with reused in mem artifacts:
//        https://github.com/akka/akka-projection/issues/198 )
sealed abstract class StatusObserverSpec extends InternalProjectionStateMetricsSpec {
  implicit var projectionId: ProjectionId = null

  before {
    projectionId = genRandomProjectionId()
  }

  def instruments(implicit projectionId: ProjectionId) = InMemInstrumentsRegistry(system).forId(projectionId)
  val defaultNumberOfEnvelopes = 6

}

class StatusObserverAtLeastOnceSpec extends StatusObserverSpec {

  "A StatusObserver reporting before and after the event handler" must {
    " in `at-least-once` with singleHandler" must {
      "reports measures for all envelopes (without afterEnvelops optimizationt )" in {
        val statusProbe = createTestProbe[TestStatusObserver.Status]()
        val beforeProbe = createTestProbe[TestStatusObserver.Before[Envelope]]()
        val afterProbe = createTestProbe[TestStatusObserver.After[Envelope]]()

        val statusObserver =
          new TestStatusObserver[Envelope](
            statusProbe.ref,
            beforeEnvelopeProbe = Some(beforeProbe.ref),
            afterEnvelopeProbe = Some(afterProbe.ref))
        val offsetStrategy = AtLeastOnce(afterEnvelopes = Some(1))
        val handler = SingleHandlerStrategy(TestHandlers.single)
        val tt = new TelemetryTester(offsetStrategy, handler, statusObserver = statusObserver)

        runInternal(tt.projectionState) {
          instruments.afterProcessInvocations.get should be(6)
        }
        beforeProbe.receiveMessages(6, 3.second) should be(
          Seq(
            TestStatusObserver.Before(Envelope(tt.entityId, 1, "a")),
            TestStatusObserver.Before(Envelope(tt.entityId, 2, "b")),
            TestStatusObserver.Before(Envelope(tt.entityId, 3, "c")),
            TestStatusObserver.Before(Envelope(tt.entityId, 4, "d")),
            TestStatusObserver.Before(Envelope(tt.entityId, 5, "e")),
            TestStatusObserver.Before(Envelope(tt.entityId, 6, "f"))))
        afterProbe.receiveMessages(6, 3.second) should be(
          Seq(
            TestStatusObserver.After(Envelope(tt.entityId, 1, "a")),
            TestStatusObserver.After(Envelope(tt.entityId, 2, "b")),
            TestStatusObserver.After(Envelope(tt.entityId, 3, "c")),
            TestStatusObserver.After(Envelope(tt.entityId, 4, "d")),
            TestStatusObserver.After(Envelope(tt.entityId, 5, "e")),
            TestStatusObserver.After(Envelope(tt.entityId, 6, "f"))))
      }
      "reports measures for all envelopes (with afterEnvelopes optimization)" in {
        val statusProbe = createTestProbe[TestStatusObserver.Status]()
        val beforeProbe = createTestProbe[TestStatusObserver.Before[Envelope]]()
        val afterProbe = createTestProbe[TestStatusObserver.After[Envelope]]()

        val statusObserver =
          new TestStatusObserver[Envelope](
            statusProbe.ref,
            beforeEnvelopeProbe = Some(beforeProbe.ref),
            afterEnvelopeProbe = Some(afterProbe.ref))

        val tt =
          new TelemetryTester(
            AtLeastOnce(afterEnvelopes = Some(3)),
            SingleHandlerStrategy(TestHandlers.single),
            statusObserver = statusObserver)
        runInternal(tt.projectionState) {
          instruments.afterProcessInvocations.get should be(6)
        }
        beforeProbe.receiveMessages(6, 3.second) should be(
          Seq(
            TestStatusObserver.Before(Envelope(tt.entityId, 1, "a")),
            TestStatusObserver.Before(Envelope(tt.entityId, 2, "b")),
            TestStatusObserver.Before(Envelope(tt.entityId, 3, "c")),
            TestStatusObserver.Before(Envelope(tt.entityId, 4, "d")),
            TestStatusObserver.Before(Envelope(tt.entityId, 5, "e")),
            TestStatusObserver.Before(Envelope(tt.entityId, 6, "f"))))
        afterProbe.receiveMessages(6, 3.second) should be(
          Seq(
            TestStatusObserver.After(Envelope(tt.entityId, 1, "a")),
            TestStatusObserver.After(Envelope(tt.entityId, 2, "b")),
            TestStatusObserver.After(Envelope(tt.entityId, 3, "c")),
            TestStatusObserver.After(Envelope(tt.entityId, 4, "d")),
            TestStatusObserver.After(Envelope(tt.entityId, 5, "e")),
            TestStatusObserver.After(Envelope(tt.entityId, 6, "f"))))

      }
      "reports measures for all envelopes (multiple times when there are failures) " in {
        val statusProbe = createTestProbe[TestStatusObserver.Status]()
        val beforeProbe = createTestProbe[TestStatusObserver.Before[Envelope]]()
        val afterProbe = createTestProbe[TestStatusObserver.After[Envelope]]()

        val statusObserver =
          new TestStatusObserver[Envelope](
            statusProbe.ref,
            beforeEnvelopeProbe = Some(beforeProbe.ref),
            afterEnvelopeProbe = Some(afterProbe.ref))
        val single = TestHandlers.singleWithErrors(3, 5)
        val numberOfEnvelopes = 6
        val tt = new TelemetryTester(
          AtLeastOnce(
            afterEnvelopes = Some(numberOfEnvelopes - 1),
            recoveryStrategy = Some(HandlerRecoveryStrategy.fail)),
          SingleHandlerStrategy(single),
          statusObserver = statusObserver)
        runInternal(tt.projectionState) {
          instruments.errorInvocations.get should be(2)
          // There's be 12 invocations to afterProcessInvocations because
          // the Errors in `singleWithErrors(3, 5)` cause the following invocations:
          //  - batch with [1,2,3,4,5] runs [1,2] and fails [3] (error(3) is dropped from the error stack)
          //  - batch with [1,2,3,4,5] runs [1,2,3,4] and fails [5] (error(5) is dropped from the error stack)
          //  - batch with [1,2,3,4,5] runs [1,2,3,4,5] and commits offset [5]
          //  - batch with [6] runs [6] and commits offset [6]
          instruments.afterProcessInvocations.get should be(2 + 4 + 6)
        }
        // There is no guarantee wrt what message will be observed in the beforeProbe
        afterProbe.receiveMessages(2 + 4 + 6, 3.second) should be(
          Seq(
            TestStatusObserver.After(Envelope(tt.entityId, 1, "a")),
            TestStatusObserver.After(Envelope(tt.entityId, 2, "b")),
            // `3` errors
            TestStatusObserver.After(Envelope(tt.entityId, 1, "a")),
            TestStatusObserver.After(Envelope(tt.entityId, 2, "b")),
            TestStatusObserver.After(Envelope(tt.entityId, 3, "c")),
            TestStatusObserver.After(Envelope(tt.entityId, 4, "d")),
            // `5 ` errors
            TestStatusObserver.After(Envelope(tt.entityId, 1, "a")),
            TestStatusObserver.After(Envelope(tt.entityId, 2, "b")),
            TestStatusObserver.After(Envelope(tt.entityId, 3, "c")),
            TestStatusObserver.After(Envelope(tt.entityId, 4, "d")),
            TestStatusObserver.After(Envelope(tt.entityId, 5, "e")),
            TestStatusObserver.After(Envelope(tt.entityId, 6, "f"))))

        statusProbe.receiveMessages(2, 3.seconds) should be(
          Seq(
            TestStatusObserver.Err(Envelope(tt.entityId, 3, "c"), TelemetryException),
            TestStatusObserver.Err(Envelope(tt.entityId, 5, "e"), TelemetryException)))

      }
    }
    " in `at-least-once` with groupedHandler" must {
      "report measures for each envelope (without afterEnvelops optimization)" in {
        val statusProbe = createTestProbe[TestStatusObserver.Status]()
        val beforeProbe = createTestProbe[TestStatusObserver.Before[Envelope]]()
        val afterProbe = createTestProbe[TestStatusObserver.After[Envelope]]()

        val statusObserver =
          new TestStatusObserver[Envelope](
            statusProbe.ref,
            beforeEnvelopeProbe = Some(beforeProbe.ref),
            afterEnvelopeProbe = Some(afterProbe.ref))
        val tt = new TelemetryTester(
          AtLeastOnce(afterEnvelopes = Some(1)),
          GroupedHandlerStrategy(TestHandlers.grouped, afterEnvelopes = Some(2), orAfterDuration = Some(30.millis)),
          statusObserver = statusObserver)

        runInternal(tt.projectionState) {
          instruments.afterProcessInvocations.get should be(defaultNumberOfEnvelopes)
        }
        beforeProbe.receiveMessages(6, 3.second) should be(
          Seq(
            TestStatusObserver.Before(Envelope(tt.entityId, 1, "a")),
            TestStatusObserver.Before(Envelope(tt.entityId, 2, "b")),
            TestStatusObserver.Before(Envelope(tt.entityId, 3, "c")),
            TestStatusObserver.Before(Envelope(tt.entityId, 4, "d")),
            TestStatusObserver.Before(Envelope(tt.entityId, 5, "e")),
            TestStatusObserver.Before(Envelope(tt.entityId, 6, "f"))))
        afterProbe.receiveMessages(6, 3.second) should be(
          Seq(
            TestStatusObserver.After(Envelope(tt.entityId, 1, "a")),
            TestStatusObserver.After(Envelope(tt.entityId, 2, "b")),
            TestStatusObserver.After(Envelope(tt.entityId, 3, "c")),
            TestStatusObserver.After(Envelope(tt.entityId, 4, "d")),
            TestStatusObserver.After(Envelope(tt.entityId, 5, "e")),
            TestStatusObserver.After(Envelope(tt.entityId, 6, "f"))))
      }

      "report multiple measures per envelope in case of failure (recovery == fail)" in {
        val statusProbe = createTestProbe[TestStatusObserver.Status]()
        val beforeProbe = createTestProbe[TestStatusObserver.Before[Envelope]]()
        val afterProbe = createTestProbe[TestStatusObserver.After[Envelope]]()

        val statusObserver =
          new TestStatusObserver[Envelope](
            statusProbe.ref,
            beforeEnvelopeProbe = Some(beforeProbe.ref),
            afterEnvelopeProbe = Some(afterProbe.ref))

        val grouped = TestHandlers.groupedWithErrors(3, 5, 6)
        // magic numbers to have at least a a batch of 4 groups of 3 envelopes (and one extra envelope)
        // 13 = 12+1 = (4*3)+1
        val numberOfEnvelopes = 13
        val tt = new TelemetryTester(
          AtLeastOnce(afterEnvelopes = Some(4), recoveryStrategy = Some(HandlerRecoveryStrategy.fail)),
          GroupedHandlerStrategy(grouped, afterEnvelopes = Some(3), orAfterDuration = Some(30.millis)),
          numberOfEnvelopes = numberOfEnvelopes,
          statusObserver = statusObserver)

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
        afterProbe.receiveMessages(0 + 3 + 3 + 12 + 1, 3.second) should be(
          Seq(
            // 1st attempt nothing is reported
            // `3` errors

            // 2nd attempt (123) are reported
            TestStatusObserver.After(Envelope(tt.entityId, 1, "a")),
            TestStatusObserver.After(Envelope(tt.entityId, 2, "b")),
            TestStatusObserver.After(Envelope(tt.entityId, 3, "c")),
            // `5` errors

            // 3rd attempt (123) are reported
            TestStatusObserver.After(Envelope(tt.entityId, 1, "a")),
            TestStatusObserver.After(Envelope(tt.entityId, 2, "b")),
            TestStatusObserver.After(Envelope(tt.entityId, 3, "c")),
            // `6` errors

            // 4rd attempt (123)...(..12) are reported
            TestStatusObserver.After(Envelope(tt.entityId, 1, "a")),
            TestStatusObserver.After(Envelope(tt.entityId, 2, "b")),
            TestStatusObserver.After(Envelope(tt.entityId, 3, "c")),
            TestStatusObserver.After(Envelope(tt.entityId, 4, "d")),
            TestStatusObserver.After(Envelope(tt.entityId, 5, "e")),
            TestStatusObserver.After(Envelope(tt.entityId, 6, "f")),
            TestStatusObserver.After(Envelope(tt.entityId, 7, "g")),
            TestStatusObserver.After(Envelope(tt.entityId, 8, "h")),
            TestStatusObserver.After(Envelope(tt.entityId, 9, "i")),
            TestStatusObserver.After(Envelope(tt.entityId, 10, "j")),
            TestStatusObserver.After(Envelope(tt.entityId, 11, "k")),
            TestStatusObserver.After(Envelope(tt.entityId, 12, "l")),
            TestStatusObserver.After(Envelope(tt.entityId, 13, "m"))))

      }

      "report multiple measures per envelope in case of failure (recovery == retryAndSkip)" in {
        val statusProbe = createTestProbe[TestStatusObserver.Status]()
        val beforeProbe = createTestProbe[TestStatusObserver.Before[Envelope]]()
        val afterProbe = createTestProbe[TestStatusObserver.After[Envelope]]()

        val statusObserver =
          new TestStatusObserver[Envelope](
            statusProbe.ref,
            beforeEnvelopeProbe = Some(beforeProbe.ref),
            afterEnvelopeProbe = Some(afterProbe.ref))

        // Envelopes 3 and 7 will error twice so they must be skipped. Note that `retries = 1`
        // means there's 1 retry _after_ an initial error so an envelope must error
        // `retries+1` times to be skipped.
        val grouped = TestHandlers.groupedWithErrors(3, 3, 6, 7, 7)
        // magic number to have at least a complete batch of 2 groups of 2 envelopes after the last error (offset==7)
        val numberOfEnvelopes = 8
        val tt = new TelemetryTester(
          AtLeastOnce(
            afterEnvelopes = Some(2),
            recoveryStrategy = Some(HandlerRecoveryStrategy.retryAndSkip(retries = 1, 10.millis))),
          GroupedHandlerStrategy(grouped, afterEnvelopes = Some(2), orAfterDuration = Some(50.millis)),
          numberOfEnvelopes = numberOfEnvelopes,
          statusObserver = statusObserver)

        runInternal(tt.projectionState) {
          // The number of invocations is 19 because:
          //  - a batch of 2 groups of 2 items is processed:
          //      [(1 2)(3 4)] but '3' errors, then it's retried, it errors again and it's skipped.
          //      report 2 envelopes (1 2)
          //  - a batch of 2 groups of 2 items is processed:
          //      [(5 6)(7 8)] but '6' errors, reties and succeeds, then 7 errors twice and is skipped
          //      report 2 envelopes (5 6)
          instruments.afterProcessInvocations.get should be(2 + 2)
        }

        afterProbe.receiveMessages(2 + 2, 3.second) should be(
          Seq(
            TestStatusObserver.After(Envelope(tt.entityId, 1, "a")),
            TestStatusObserver.After(Envelope(tt.entityId, 2, "b")),
            TestStatusObserver.After(Envelope(tt.entityId, 5, "e")),
            TestStatusObserver.After(Envelope(tt.entityId, 6, "f"))))
      }

    }
    " in `at-least-once` with flowHandler" must {
      "report a measure per envelope" in {
        val statusProbe = createTestProbe[TestStatusObserver.Status]()
        val beforeProbe = createTestProbe[TestStatusObserver.Before[Envelope]]()
        val afterProbe = createTestProbe[TestStatusObserver.After[Envelope]]()

        val statusObserver =
          new TestStatusObserver[Envelope](
            statusProbe.ref,
            beforeEnvelopeProbe = Some(beforeProbe.ref),
            afterEnvelopeProbe = Some(afterProbe.ref))
        val tt =
          new TelemetryTester(
            AtLeastOnce(afterEnvelopes = Some(3)),
            FlowHandlerStrategy[Envelope](TestHandlers.flow),
            statusObserver = statusObserver)

        runInternal(tt.projectionState) {
          instruments.afterProcessInvocations.get should be(defaultNumberOfEnvelopes)
        }
        beforeProbe.receiveMessages(6, 3.second) should be(
          Seq(
            TestStatusObserver.Before(Envelope(tt.entityId, 1, "a")),
            TestStatusObserver.Before(Envelope(tt.entityId, 2, "b")),
            TestStatusObserver.Before(Envelope(tt.entityId, 3, "c")),
            TestStatusObserver.Before(Envelope(tt.entityId, 4, "d")),
            TestStatusObserver.Before(Envelope(tt.entityId, 5, "e")),
            TestStatusObserver.Before(Envelope(tt.entityId, 6, "f"))))
        afterProbe.receiveMessages(6, 3.second) should be(
          Seq(
            TestStatusObserver.After(Envelope(tt.entityId, 1, "a")),
            TestStatusObserver.After(Envelope(tt.entityId, 2, "b")),
            TestStatusObserver.After(Envelope(tt.entityId, 3, "c")),
            TestStatusObserver.After(Envelope(tt.entityId, 4, "d")),
            TestStatusObserver.After(Envelope(tt.entityId, 5, "e")),
            TestStatusObserver.After(Envelope(tt.entityId, 6, "f"))))
      }
      "report multiple measures per envelope in case of failure" in {
        val statusProbe = createTestProbe[TestStatusObserver.Status]()
        val beforeProbe = createTestProbe[TestStatusObserver.Before[Envelope]]()
        val afterProbe = createTestProbe[TestStatusObserver.After[Envelope]]()

        val statusObserver =
          new TestStatusObserver[Envelope](
            statusProbe.ref,
            beforeEnvelopeProbe = Some(beforeProbe.ref),
            afterEnvelopeProbe = Some(afterProbe.ref))
        val flow = TestHandlers.flowWithErrors(2, 5, 6)
        val tt =
          new TelemetryTester(
            AtLeastOnce(afterEnvelopes = Some(2)),
            FlowHandlerStrategy[Envelope](flow),
            statusObserver = statusObserver)
        runInternal(tt.projectionState) {
          // When there's a failure handling `envelope(n)` there is a race condition between
          // the stream cancellation and the invocation to `afterProcess(envelope(n-1))` (the
          // previous envelope). As a consequence, the `afterProcessInvocations` count is
          // nondeterministic and we can only assert it'll be some value between 8 and 10 (both
          // included)
          instruments.afterProcessInvocations.get should be >= (0 + 4 + 2 + 2)
          instruments.afterProcessInvocations.get should be <= (1 + 4 + 3 + 2)
        }

        // Because there are 3 errors, the projection runs 4 times.
        // 1) may or may not invoke `afterProcess` for envelope [1]
        // 2) will invoke `afterProcess` for envelopes [1,2,3,4]
        // 3) will invoke `afterProcess` for envelopes [3,4] (and maybe [5])
        // 4) will invoke `afterProcess` for envelopes [5,6]

        // The only thing we can guarantee is that the first invocation
        // will be `After(Envelope(1)`
        afterProbe.receiveMessage(3.second) should be(TestStatusObserver.After(Envelope(tt.entityId, 1, "a")))
      }
    }
  }
}

class StatusObserverExactlyOnceSpec extends StatusObserverSpec {

  "A metric reporting ServiceTime" must {

    // exactly-once
    " in `exactly-once` with singleHandler" must {
      "report only one measure per envelope" in {
        val statusProbe = createTestProbe[TestStatusObserver.Status]()
        val beforeProbe = createTestProbe[TestStatusObserver.Before[Envelope]]()
        val afterProbe = createTestProbe[TestStatusObserver.After[Envelope]]()

        val statusObserver =
          new TestStatusObserver[Envelope](
            statusProbe.ref,
            beforeEnvelopeProbe = Some(beforeProbe.ref),
            afterEnvelopeProbe = Some(afterProbe.ref))

        val tt =
          new TelemetryTester(
            ExactlyOnce(),
            SingleHandlerStrategy(TestHandlers.single),
            statusObserver = statusObserver)

        runInternal(tt.projectionState) {
          instruments.afterProcessInvocations.get should be(defaultNumberOfEnvelopes)
        }
        beforeProbe.receiveMessages(6, 3.second) should be(
          Seq(
            TestStatusObserver.Before(Envelope(tt.entityId, 1, "a")),
            TestStatusObserver.Before(Envelope(tt.entityId, 2, "b")),
            TestStatusObserver.Before(Envelope(tt.entityId, 3, "c")),
            TestStatusObserver.Before(Envelope(tt.entityId, 4, "d")),
            TestStatusObserver.Before(Envelope(tt.entityId, 5, "e")),
            TestStatusObserver.Before(Envelope(tt.entityId, 6, "f"))))
        afterProbe.receiveMessages(6, 3.second) should be(
          Seq(
            TestStatusObserver.After(Envelope(tt.entityId, 1, "a")),
            TestStatusObserver.After(Envelope(tt.entityId, 2, "b")),
            TestStatusObserver.After(Envelope(tt.entityId, 3, "c")),
            TestStatusObserver.After(Envelope(tt.entityId, 4, "d")),
            TestStatusObserver.After(Envelope(tt.entityId, 5, "e")),
            TestStatusObserver.After(Envelope(tt.entityId, 6, "f"))))
      }

      "report only one measure per envelope even in case of failure" in {
        val statusProbe = createTestProbe[TestStatusObserver.Status]()
        val beforeProbe = createTestProbe[TestStatusObserver.Before[Envelope]]()
        val afterProbe = createTestProbe[TestStatusObserver.After[Envelope]]()

        val statusObserver =
          new TestStatusObserver[Envelope](
            statusProbe.ref,
            beforeEnvelopeProbe = Some(beforeProbe.ref),
            afterEnvelopeProbe = Some(afterProbe.ref))

        val single = TestHandlers.singleWithErrors(2, 2, 2, 3, 3, 3, 5)
        val tt = new TelemetryTester(
          // using retryAndFail to try to get all message through
          ExactlyOnce(recoveryStrategy = Some(HandlerRecoveryStrategy.fail)),
          SingleHandlerStrategy(single),
          statusObserver = statusObserver)

        runInternal(tt.projectionState) {
          instruments.afterProcessInvocations.get should be(defaultNumberOfEnvelopes)
        }
        afterProbe.receiveMessages(6, 3.second) should be(
          Seq(
            TestStatusObserver.After(Envelope(tt.entityId, 1, "a")),
            TestStatusObserver.After(Envelope(tt.entityId, 2, "b")),
            TestStatusObserver.After(Envelope(tt.entityId, 3, "c")),
            TestStatusObserver.After(Envelope(tt.entityId, 4, "d")),
            TestStatusObserver.After(Envelope(tt.entityId, 5, "e")),
            TestStatusObserver.After(Envelope(tt.entityId, 6, "f"))))
      }
    }
    " in `exactly-once` with groupedHandler" must {
      "report only one measure per envelope" in {
        val statusProbe = createTestProbe[TestStatusObserver.Status]()
        val beforeProbe = createTestProbe[TestStatusObserver.Before[Envelope]]()
        val afterProbe = createTestProbe[TestStatusObserver.After[Envelope]]()

        val statusObserver =
          new TestStatusObserver[Envelope](
            statusProbe.ref,
            beforeEnvelopeProbe = Some(beforeProbe.ref),
            afterEnvelopeProbe = Some(afterProbe.ref))
        val grouped = TestHandlers.grouped
        val groupHandler = GroupedHandlerStrategy(grouped, afterEnvelopes = Some(2), orAfterDuration = Some(30.millis))
        val tt =
          new TelemetryTester(ExactlyOnce(), groupHandler, statusObserver = statusObserver)
        runInternal(tt.projectionState) {
          instruments.afterProcessInvocations.get should be(defaultNumberOfEnvelopes)
        }
        beforeProbe.receiveMessages(6, 3.second) should be(
          Seq(
            TestStatusObserver.Before(Envelope(tt.entityId, 1, "a")),
            TestStatusObserver.Before(Envelope(tt.entityId, 2, "b")),
            TestStatusObserver.Before(Envelope(tt.entityId, 3, "c")),
            TestStatusObserver.Before(Envelope(tt.entityId, 4, "d")),
            TestStatusObserver.Before(Envelope(tt.entityId, 5, "e")),
            TestStatusObserver.Before(Envelope(tt.entityId, 6, "f"))))
        afterProbe.receiveMessages(6, 3.second) should be(
          Seq(
            TestStatusObserver.After(Envelope(tt.entityId, 1, "a")),
            TestStatusObserver.After(Envelope(tt.entityId, 2, "b")),
            TestStatusObserver.After(Envelope(tt.entityId, 3, "c")),
            TestStatusObserver.After(Envelope(tt.entityId, 4, "d")),
            TestStatusObserver.After(Envelope(tt.entityId, 5, "e")),
            TestStatusObserver.After(Envelope(tt.entityId, 6, "f"))))
      }

      "report only one measure per envelope even in case of failure (recovery is fail)" in {
        val statusProbe = createTestProbe[TestStatusObserver.Status]()
        val beforeProbe = createTestProbe[TestStatusObserver.Before[Envelope]]()
        val afterProbe = createTestProbe[TestStatusObserver.After[Envelope]]()

        val statusObserver =
          new TestStatusObserver[Envelope](
            statusProbe.ref,
            beforeEnvelopeProbe = Some(beforeProbe.ref),
            afterEnvelopeProbe = Some(afterProbe.ref))

        val groupedWithFailures = TestHandlers.groupedWithErrors(5)
        val tt = new TelemetryTester(
          ExactlyOnce(recoveryStrategy = Some(HandlerRecoveryStrategy.fail)),
          GroupedHandlerStrategy(groupedWithFailures, afterEnvelopes = Some(2), orAfterDuration = Some(10.millis)),
          statusObserver = statusObserver)
        runInternal(tt.projectionState) {
          instruments.afterProcessInvocations.get should be(defaultNumberOfEnvelopes)
        }
        afterProbe.receiveMessages(6, 3.second) should be(
          Seq(
            TestStatusObserver.After(Envelope(tt.entityId, 1, "a")),
            TestStatusObserver.After(Envelope(tt.entityId, 2, "b")),
            TestStatusObserver.After(Envelope(tt.entityId, 3, "c")),
            TestStatusObserver.After(Envelope(tt.entityId, 4, "d")),
            TestStatusObserver.After(Envelope(tt.entityId, 5, "e")),
            TestStatusObserver.After(Envelope(tt.entityId, 6, "f"))))

      }

      "report only one measure per envelope even in case of failure (recovery is skip)" in {
        val statusProbe = createTestProbe[TestStatusObserver.Status]()
        val beforeProbe = createTestProbe[TestStatusObserver.Before[Envelope]]()
        val afterProbe = createTestProbe[TestStatusObserver.After[Envelope]]()

        val statusObserver =
          new TestStatusObserver[Envelope](
            statusProbe.ref,
            beforeEnvelopeProbe = Some(beforeProbe.ref),
            afterEnvelopeProbe = Some(afterProbe.ref))

        val groupedWithFailures = TestHandlers.groupedWithErrors(1, 7)
        val numberOfEnvelopes = 11
        val tt = new TelemetryTester(
          ExactlyOnce(recoveryStrategy = Some(HandlerRecoveryStrategy.skip)),
          GroupedHandlerStrategy(groupedWithFailures, afterEnvelopes = Some(2), orAfterDuration = Some(10.millis)),
          numberOfEnvelopes,
          statusObserver = statusObserver)
        runInternal(tt.projectionState) {
          // [(12)] -> 0
          // [(34)] -> 2
          // [(56)] -> 2
          // [(78)] -> 0
          // [(9 10)] -> 2
          // [(11)] -> 1
          instruments.afterProcessInvocations.get should be(0 + 2 + 2 + 0 + 2 + 1)
        }
        afterProbe.receiveMessages(0 + 2 + 2 + 0 + 2 + 1, 3.second) should be(
          Seq(
            // (12) are not reported
            // (34) are reported
            TestStatusObserver.After(Envelope(tt.entityId, 3, "c")),
            TestStatusObserver.After(Envelope(tt.entityId, 4, "d")),
            // (56) are reported
            TestStatusObserver.After(Envelope(tt.entityId, 5, "e")),
            TestStatusObserver.After(Envelope(tt.entityId, 6, "f")),
            // (78) are not reported
            // (9 10) are reported
            TestStatusObserver.After(Envelope(tt.entityId, 9, "i")),
            TestStatusObserver.After(Envelope(tt.entityId, 10, "j")),
            // (11) are reported
            TestStatusObserver.After(Envelope(tt.entityId, 11, "k"))))
      }
    }
  }

}

class StatusObserverAtMostOnceSpec extends StatusObserverSpec {

  "A metric reporting ServiceTime" must {

    // at-most-once
    " in `at-most-once` with singleHandler" must {
      "report measures" in {
        val statusProbe = createTestProbe[TestStatusObserver.Status]()
        val beforeProbe = createTestProbe[TestStatusObserver.Before[Envelope]]()
        val afterProbe = createTestProbe[TestStatusObserver.After[Envelope]]()

        val statusObserver =
          new TestStatusObserver[Envelope](
            statusProbe.ref,
            beforeEnvelopeProbe = Some(beforeProbe.ref),
            afterEnvelopeProbe = Some(afterProbe.ref))

        val tt =
          new TelemetryTester(AtMostOnce(), SingleHandlerStrategy(TestHandlers.single), statusObserver = statusObserver)
        runInternal(tt.projectionState) {
          instruments.afterProcessInvocations.get should be(defaultNumberOfEnvelopes)
        }
        beforeProbe.receiveMessages(6, 3.second) should be(
          Seq(
            TestStatusObserver.Before(Envelope(tt.entityId, 1, "a")),
            TestStatusObserver.Before(Envelope(tt.entityId, 2, "b")),
            TestStatusObserver.Before(Envelope(tt.entityId, 3, "c")),
            TestStatusObserver.Before(Envelope(tt.entityId, 4, "d")),
            TestStatusObserver.Before(Envelope(tt.entityId, 5, "e")),
            TestStatusObserver.Before(Envelope(tt.entityId, 6, "f"))))
        afterProbe.receiveMessages(6, 3.second) should be(
          Seq(
            TestStatusObserver.After(Envelope(tt.entityId, 1, "a")),
            TestStatusObserver.After(Envelope(tt.entityId, 2, "b")),
            TestStatusObserver.After(Envelope(tt.entityId, 3, "c")),
            TestStatusObserver.After(Envelope(tt.entityId, 4, "d")),
            TestStatusObserver.After(Envelope(tt.entityId, 5, "e")),
            TestStatusObserver.After(Envelope(tt.entityId, 6, "f"))))

      }
      "report measures if envelopes were processed in case of failure" in {
        val statusProbe = createTestProbe[TestStatusObserver.Status]()
        val beforeProbe = createTestProbe[TestStatusObserver.Before[Envelope]]()
        val afterProbe = createTestProbe[TestStatusObserver.After[Envelope]]()

        val statusObserver =
          new TestStatusObserver[Envelope](
            statusProbe.ref,
            beforeEnvelopeProbe = Some(beforeProbe.ref),
            afterEnvelopeProbe = Some(afterProbe.ref))
        val single = TestHandlers.singleWithErrors(4, 5, 6)
        val numberOfEnvelopes = 100
        val tt = new TelemetryTester(
          AtMostOnce(recoveryStrategy = Some(HandlerRecoveryStrategy.fail)),
          SingleHandlerStrategy(single),
          numberOfEnvelopes,
          statusObserver = statusObserver)

        runInternal(tt.projectionState) {
          instruments.afterProcessInvocations.get should be(97)
        }

        afterProbe.receiveMessages(97, 3.second).take(5) should be(
          Seq(
            TestStatusObserver.After(Envelope(tt.entityId, 1, "a")),
            TestStatusObserver.After(Envelope(tt.entityId, 2, "b")),
            TestStatusObserver.After(Envelope(tt.entityId, 3, "c")),
            TestStatusObserver.After(Envelope(tt.entityId, 7, "g")),
            TestStatusObserver.After(Envelope(tt.entityId, 8, "h"))))

      }
    }

  }

}
