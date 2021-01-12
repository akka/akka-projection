/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

// #testKitSpec
package jdocs.guide;

import akka.Done;
import akka.NotUsed;
import akka.actor.testkit.typed.javadsl.LoggingTestKit;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.persistence.query.Offset;
import akka.projection.ProjectionId;
import akka.projection.eventsourced.EventEnvelope;
import akka.projection.javadsl.Handler;
import akka.projection.javadsl.SourceProvider;
// #testKitImports
import akka.projection.testkit.javadsl.ProjectionTestKit;
import akka.projection.testkit.javadsl.TestProjection;
import akka.projection.testkit.javadsl.TestSourceProvider;
// #testKitImports
import akka.stream.javadsl.Source;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class ShoppingCartAppTest {
  @ClassRule public static final TestKitJunitResource testKit = new TestKitJunitResource();
  public static final ProjectionTestKit projectionTestKit =
      ProjectionTestKit.create(testKit.system());

  EventEnvelope<ShoppingCartEvents.Event> createEnvelope(
      ShoppingCartEvents.Event event, Long seqNo, Long timestamp) {
    return EventEnvelope.create(Offset.sequence(seqNo), "persistenceId", seqNo, event, timestamp);
  }

  @Test
  public void projectionHandlerShouldProcessItemEventsCorrectly() {
    MockItemPopularityRepository repo = new MockItemPopularityRepository();
    Handler<EventEnvelope<ShoppingCartEvents.Event>> handler =
        new ItemPopularityProjectionHandler("tag", testKit.system(), repo);

    Source<EventEnvelope<ShoppingCartEvents.Event>, NotUsed> events =
        Source.from(
            Arrays.asList(
                createEnvelope(
                    new ShoppingCartEvents.ItemAdded("a7098", "bowling shoes", 1), 0L, 0L),
                createEnvelope(
                    new ShoppingCartEvents.ItemQuantityAdjusted("a7098", "bowling shoes", 2, 1),
                    1L,
                    0L),
                createEnvelope(
                    new ShoppingCartEvents.CheckedOut(
                        "a7098", Instant.parse("2020-01-01T12:00:00.00Z")),
                    2L,
                    0L),
                createEnvelope(
                    new ShoppingCartEvents.ItemAdded("0d12d", "akka t-shirt", 1), 3L, 0L),
                createEnvelope(new ShoppingCartEvents.ItemAdded("0d12d", "skis", 1), 4L, 0L),
                createEnvelope(new ShoppingCartEvents.ItemRemoved("0d12d", "skis", 1), 5L, 0L),
                createEnvelope(
                    new ShoppingCartEvents.CheckedOut(
                        "0d12d", Instant.parse("2020-01-01T12:05:00.00Z")),
                    6L,
                    0L)));

    ProjectionId projectionId = ProjectionId.of("name", "key");
    SourceProvider<Offset, EventEnvelope<ShoppingCartEvents.Event>> sourceProvider =
        TestSourceProvider.create(events, env -> env.offset());
    TestProjection<Offset, EventEnvelope<ShoppingCartEvents.Event>> projection =
        TestProjection.create(projectionId, sourceProvider, () -> handler);

    projectionTestKit.run(
        projection,
        () -> {
          assertEquals(repo.counts.size(), 3);
          assertEquals(repo.counts.get("bowling shoes"), Long.valueOf(2L));
          assertEquals(repo.counts.get("akka t-shirt"), Long.valueOf(1L));
          assertEquals(repo.counts.get("skis"), Long.valueOf(0L));
        });
  }

  @Test
  public void projectionHandlerShouldLogItemPopularityEvery10Events() {
    long eventsNum = 10L;
    MockItemPopularityRepository repo = new MockItemPopularityRepository();
    Handler<EventEnvelope<ShoppingCartEvents.Event>> handler =
        new ItemPopularityProjectionHandler("tag", testKit.system(), repo);

    Source<EventEnvelope<ShoppingCartEvents.Event>, NotUsed> events =
        Source.fromJavaStream(
            () ->
                IntStream.range(0, (int) eventsNum)
                    .boxed()
                    .map(
                        i ->
                            createEnvelope(
                                new ShoppingCartEvents.ItemAdded("a7098", "bowling shoes", 1),
                                Long.valueOf(i),
                                0L)));

    ProjectionId projectionId = ProjectionId.of("name", "key");
    SourceProvider<Offset, EventEnvelope<ShoppingCartEvents.Event>> sourceProvider =
        TestSourceProvider.create(events, env -> env.offset());
    TestProjection<Offset, EventEnvelope<ShoppingCartEvents.Event>> projection =
        TestProjection.create(projectionId, sourceProvider, () -> handler);

    LoggingTestKit.info(
            "ItemPopularityProjectionHandler(tag) item popularity for 'bowling shoes': [10]")
        .expect(
            testKit.system(),
            () -> {
              projectionTestKit.runWithTestSink(
                  projection,
                  testSink -> {
                    testSink.request(eventsNum);
                    testSink.expectNextN(eventsNum);
                  });
              return null; // FIXME: why is a return statement required?
            });
  }

  static class MockItemPopularityRepository implements ItemPopularityProjectionRepository {
    public Map<String, Long> counts = new HashMap<String, Long>();

    @Override
    public CompletionStage<Done> update(String itemId, int delta) {
      counts.put(itemId, counts.getOrDefault(itemId, 0L) + delta);
      return CompletableFuture.completedFuture(Done.getInstance());
    }

    @Override
    public CompletionStage<Optional<Long>> getItem(String itemId) {
      if (counts.containsKey(itemId))
        return CompletableFuture.completedFuture(Optional.of(counts.get(itemId)));

      return CompletableFuture.completedFuture(Optional.empty());
    }
  }
}
// #testKitSpec
