/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection;

import akka.Done;
import akka.actor.ClassicActorSystemProvider;
import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.typed.ActorRef;
import akka.stream.scaladsl.Source;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

/**
 * Compile test: this class serves only for exercising the Java API.
 */
public class ProjectionBehaviorCompileTest {

    public void compileTest() {
        ActorTestKit testKit = ActorTestKit.create();
        ActorRef<ProjectionBehavior.Command> ref =
                testKit.spawn(ProjectionBehavior.create(TestProjection::new));
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
        public Source<Done, ?> mappedSource(ClassicActorSystemProvider systemProvider) {
            return null;
        }

        @Override
        public void run(ClassicActorSystemProvider systemProvider) {

        }

        @Override
        public Future<Done> stop(ExecutionContext ec) {
            return null;
        }

        @Override
        public Projection<String> withSettings(ProjectionSettings projectionSettings) {
            // no need for ProjectionSettings in tests
            return this;
        }
    }
}
