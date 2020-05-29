/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection;

import akka.Done;
import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.stream.scaladsl.Source;

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
        public RunningProjection run(ActorSystem<?> system) {
            return null;
        }



        @Override
        public Projection<String> withSettings(ProjectionSettings settings) {
            // no need for ProjectionSettings in tests
            return this;
        }
    }
}
