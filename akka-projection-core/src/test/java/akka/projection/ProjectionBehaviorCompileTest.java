/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection;

import akka.Done;
import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.projection.internal.ActorHandlerInit;
import akka.projection.internal.NoopStatusObserver;
import akka.projection.internal.ProjectionSettings;
import akka.stream.scaladsl.Source;
import scala.Option;
import scala.concurrent.duration.FiniteDuration;

import java.time.Duration;

/**
 * Compile test: this class serves only for exercising the Java API.
 */
public class ProjectionBehaviorCompileTest {

    public void compileTest() {
        ActorTestKit testKit = ActorTestKit.create();
        ActorRef<ProjectionBehavior.Command> ref =
                testKit.spawn(ProjectionBehavior.create(new TestProjection()));
        ref.tell(ProjectionBehavior.stopMessage());
        // nobody is calling this method, so not really starting the system,
        // but we never know
        testKit.shutdownTestKit();
    }

    static class TestProjection implements Projection<String> {

        @Override
        public ProjectionId projectionId() {
            return null;
        }

        @Override
        public Source<Done, ?> mappedSource(ActorSystem<?> system) {
            return null;
        }

        @Override
        public <M> Option<ActorHandlerInit<M>> actorHandlerInit() {
            return Option.empty();
        }

        @Override
        public RunningProjection run(ActorSystem<?> system) {
            return null;
        }

        @Override
        public Projection<String> withSettings(ProjectionSettings settings) {
            // no need for ProjectionSettings in tests
            return this;
        }

        @Override
        public StatusObserver<String> statusObserver() {
            return NoopStatusObserver.getInstance();
        }

        @Override
        public Projection<String> withStatusObserver(StatusObserver<String> observer) {
            // no need for StatusObserver in tests
            return this;
        }

        @Override
        public Projection<String> withRestartBackoff(FiniteDuration minBackoff, FiniteDuration maxBackoff, double randomFactor) {
            return this;
        }

        @Override
        public Projection<String> withRestartBackoff(FiniteDuration minBackoff, FiniteDuration maxBackoff, double randomFactor, int maxRestarts) {
            return this;
        }

        @Override
        public Projection<String> withRestartBackoff(Duration minBackoff, Duration maxBackoff, double randomFactor) {
            return this;
        }

        @Override
        public Projection<String> withRestartBackoff(Duration minBackoff, Duration maxBackoff, double randomFactor, int maxRestarts) {
            return this;
        }
    }
}
