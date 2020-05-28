/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.testkit;

import akka.Done;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.typed.ActorSystem;
import akka.projection.Projection;
import akka.projection.ProjectionId;
import akka.projection.ProjectionSettings;
import akka.projection.RunningProjection;
import akka.projection.testkit.javadsl.ProjectionTestKit;
import akka.stream.scaladsl.Source;
import org.junit.ClassRule;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

//#testkit-import
//#testkit-import
//#testkit-duration
//#testkit-duration
//#testkit-assertion-import

//#testkit-assertion-import

public class TestKitDocExample {

  static class CartView {
    final String id;

    CartView(String id) {
      this.id = id;
    }
  }
  static class CartCheckoutRepository {
    public CompletionStage<CartView> findById(String id) {
      return CompletableFuture.completedFuture(new CartView(id));
    }
  }

  //#testkit
  @ClassRule
  static final TestKitJunitResource testKit = new TestKitJunitResource();
  ProjectionTestKit projectionTestKit = ProjectionTestKit.create(testKit.testKit());
  //#testkit



  Projection<String> projection = new Projection<String>() {
    @Override
    public ProjectionId projectionId() {
      return null;
    }

    @Override
    public Projection<String> withSettings(ProjectionSettings settings) {
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
  };

  CartCheckoutRepository cartCheckoutRepository = new CartCheckoutRepository();

  void illustrateTestKitRun() {
    //#testkit-run
    projectionTestKit.run(projection, () ->
      cartCheckoutRepository
        .findById("abc-def")
        .toCompletableFuture().get(1,TimeUnit.SECONDS));
    //#testkit-run
  }

  void illustrateTestKitRunWithMaxAndInterval() {
    //#testkit-run-max-interval
    projectionTestKit.run(projection, Duration.ofSeconds(5), Duration.ofMillis(300), () ->
      cartCheckoutRepository
        .findById("abc-def")
        .toCompletableFuture().get(1, TimeUnit.SECONDS));
    //#testkit-run-max-interval
  }


  void illustrateTestKitRunWithTestSink() {

    //#testkit-sink-probe
    projectionTestKit.runWithTestSink(projection, sinkProbe -> {
      sinkProbe.request(1);
      sinkProbe.expectNext(Done.getInstance());
      cartCheckoutRepository
        .findById("abc-def")
        .toCompletableFuture().get(1, TimeUnit.SECONDS);
    });

    //#testkit-sink-probe
  }

  //#fixme
  //FIXME: Java example
  //#fixme
}
