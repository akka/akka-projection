/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit.javadsl;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import akka.projection.*;
import org.junit.*;
import scala.compat.java8.FutureConverters;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

import akka.Done;
import akka.NotUsed;
import akka.actor.ClassicActorSystemProvider;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.japi.function.Function;
import akka.stream.DelayOverflowStrategy;
import akka.stream.KillSwitches;
import akka.stream.SharedKillSwitch;
import akka.stream.javadsl.DelayStrategy;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.TestSubscriber;
import org.scalatestplus.junit.JUnitSuite;

import static org.junit.Assert.assertEquals;

public class ProjectionTestKitTest extends JUnitSuite {

    private List<Integer> elements = IntStream.rangeClosed(1, 20)
            .boxed().collect(Collectors.toList());

    private Source<Integer, NotUsed> src = Source.from(elements);

    @ClassRule
    public static final TestKitJunitResource testKitJunit = new TestKitJunitResource();


    private ProjectionTestKit projectionTestKit = new ProjectionTestKit(testKitJunit.testKit());

    @Test
    public void assertProgressOfAProjection() {
        StringBuffer strBuffer = new StringBuffer("");
        TestProjection prj = new TestProjection(src, strBuffer, i -> i <= 6);

        projectionTestKit.run(prj, () -> assertEquals(strBuffer.toString(), "1-2-3-4-5-6"));
    }

    @Test
    public void retryAssertionFunctionUntilItSucceedsWithinAMaxTimeout() {

        StringBuffer strBuffer = new StringBuffer("");

        Source<Integer, NotUsed> delayedSrc = src.delayWith(
                () -> DelayStrategy.linearIncreasingDelay(Duration.ofMillis(200), __ -> true),
                DelayOverflowStrategy.backpressure());

        TestProjection prj = new TestProjection(delayedSrc, strBuffer, i -> i <= 6);

        projectionTestKit.run(prj, Duration.ofSeconds(2), () -> {
            assertEquals(strBuffer.toString(), "1-2-3-4-5-6");
        });
    }

    @Test
    public void retryAssertionFunctionAndFailWhenTimeoutExpires() {
        StringBuffer strBuffer = new StringBuffer("");

        Source<Integer, NotUsed> delayedSrc = src.delayWith(
                () -> DelayStrategy.linearIncreasingDelay(Duration.ofMillis(1000), __ -> true),
                DelayOverflowStrategy.backpressure());

        TestProjection prj = new TestProjection(delayedSrc, strBuffer, i -> i <= 2);

        try {
            projectionTestKit.run(prj, Duration.ofSeconds(1), () -> {
                assertEquals(strBuffer.toString(), "1-2");
            });
            Assert.fail("should not reach that line");
        } catch (ComparisonFailure failure) {
            // that was expected
        }
    }

    @Test
    public void failureInsideProjectionPropagatesToTestkit() {

        String streamFailureMsg = "stream failure";
        StringBuffer strBuffer = new StringBuffer("");

        TestProjection prj = new TestProjection(src, strBuffer, i -> {
         if (i < 3) return true;
         else throw new RuntimeException(streamFailureMsg);
        });

        try {
            projectionTestKit.run(prj, () -> assertEquals(strBuffer.toString(), "1-2-3-4"));
            Assert.fail("should not reach that line");
        } catch (RuntimeException ex) {
            assertEquals(ex.getMessage(), streamFailureMsg);
        }

    }

    @Test
    public void failureInsideStreamPropagatesToTestkit() {

        String streamFailureMsg = "stream failure";
        StringBuffer strBuffer = new StringBuffer("");

        Source<Integer, NotUsed> failingSource =
                Source.single(1).concat(Source.failed(new RuntimeException(streamFailureMsg)));

        TestProjection prj = new TestProjection(failingSource, strBuffer, i ->  i <= 4);

        try {
            projectionTestKit.run(prj, () -> assertEquals(strBuffer.toString(), "1-2-3-4"));
            Assert.fail("should not reach that line");
        } catch (RuntimeException ex) {
            assertEquals(ex.getMessage(), streamFailureMsg);
        }
    }

    @Test
    public void runAProjectionWithATestSink() {

        StringBuffer strBuffer = new StringBuffer("");
        List<Integer> elements = IntStream.rangeClosed(1, 5)
                .boxed().collect(Collectors.toList());

        TestProjection prj = new TestProjection(Source.from(elements), strBuffer, i -> i <= 5);
        TestSubscriber.Probe<Done> sinkProbe = projectionTestKit.runWithTestSink(prj);

        sinkProbe.request(5);
        sinkProbe.expectNextN(5);
        sinkProbe.expectComplete();

        assertEquals(strBuffer.toString(), "1-2-3-4-5");

    }


    private class TestProjection implements Projection<Integer> {

        final private Source<Integer, NotUsed> src;
        final private StringBuffer strBuffer;
        final private Predicate<Integer> predicate;

        private TestProjection(Source<Integer, NotUsed> src, StringBuffer strBuffer, Predicate<Integer> predicate) {
            this.src = src;
            this.strBuffer = strBuffer;
            this.predicate = predicate;
        }


        @Override
        public ProjectionId projectionId() {
            return ProjectionId.of("test-projection", "00");
        }

        @Override
        public akka.stream.scaladsl.Source<Done, NotUsed> mappedSource(ClassicActorSystemProvider systemProvider) {
            return new InternalProjectionState(strBuffer, predicate, systemProvider).mappedSource();
        }


        @Override
        public RunningProjection run(ClassicActorSystemProvider systemProvider) {
            return new InternalProjectionState(strBuffer, predicate, systemProvider).newRunningInstance();
        }

        @Override
        public Projection<Integer> withSettings(ProjectionSettings settings) {
            // no need for ProjectionSettings in tests
            return this;
        }

        /*
         * INTERNAL API
         * This internal class will hold the KillSwitch that is needed
         * when building the mappedSource and when running the projection (to stop)
         */
        private class InternalProjectionState {

            final private ClassicActorSystemProvider systemProvider;
            final private SharedKillSwitch killSwitch;
            final private StringBuffer strBuffer;
            final private Predicate<Integer> predicate;

            private InternalProjectionState(StringBuffer strBuffer, Predicate<Integer> predicate, ClassicActorSystemProvider systemProvider) {
                this.strBuffer = strBuffer;
                this.predicate = predicate;
                this.systemProvider = systemProvider;
                this.killSwitch = KillSwitches.shared(TestProjection.this.projectionId().id());
            }

            private CompletionStage<Done> process(Integer i) {
                if (predicate.test(i)) {
                    if (strBuffer.toString().isEmpty())
                        strBuffer.append(i);
                    else
                        strBuffer.append("-").append(i);
                }
                return CompletableFuture.completedFuture(Done.getInstance());
            }


            private akka.stream.scaladsl.Source<Done, NotUsed> mappedSource() {
                return src.via(killSwitch.flow())
                        .mapAsync(1, (Function<Integer, CompletionStage<Done>>) this::process).asScala();
            }

            private RunningProjection newRunningInstance() {
                return new TestRunningProjection(mappedSource(), killSwitch, systemProvider);
            }
        }

        private class TestRunningProjection implements RunningProjection {

            final private SharedKillSwitch killSwitch;
            final private Future<Done> futureDone;

            private TestRunningProjection(akka.stream.scaladsl.Source<Done, NotUsed> source, SharedKillSwitch killSwitch, ClassicActorSystemProvider systemProvider) {
                this.killSwitch = killSwitch;
                CompletionStage<Done> done = source.asJava().runWith(Sink.ignore(), systemProvider);
                this.futureDone = FutureConverters.toScala(done);
            }

            @Override
            public Future<Done> stop(ExecutionContext ec) {
                killSwitch.shutdown();
                return this.futureDone;
            }
        }
    }
}
