/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra;

import akka.Done;
import akka.actor.testkit.typed.javadsl.LogCapturing;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.javadsl.Behaviors;
import akka.projection.Projection;
import akka.projection.ProjectionBehavior;
import akka.projection.ProjectionId;
import akka.projection.cassandra.internal.CassandraOffsetStore;
import akka.projection.cassandra.javadsl.CassandraProjection;
import akka.projection.javadsl.ActorHandler;
import akka.projection.javadsl.Handler;
import akka.projection.javadsl.SourceProvider;
import akka.projection.testkit.javadsl.ProjectionTestKit;
import akka.stream.alpakka.cassandra.javadsl.CassandraSession;
import akka.stream.alpakka.cassandra.javadsl.CassandraSessionRegistry;
import akka.stream.javadsl.Source;
import org.junit.*;
import org.scalatestplus.junit.JUnitSuite;
import scala.concurrent.Await;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;

public class CassandraProjectionTest extends JUnitSuite {
  @ClassRule
  public static final TestKitJunitResource testKit = new TestKitJunitResource();

  @Rule
  public final LogCapturing logCapturing = new LogCapturing();

  private static CassandraSession session;
  private static CassandraOffsetStore offsetStore;

  @BeforeClass
  public static void beforeAll() throws Exception {
    // don't use futureValue (patience) here because it can take a while to start the test container
    Await.result(ContainerSessionProvider.started(), scala.concurrent.duration.Duration.create(30, TimeUnit.SECONDS));

    offsetStore = new CassandraOffsetStore(testKit.system());
    session = CassandraSessionRegistry.get(testKit.system()).sessionFor("akka.projection.cassandra.session-config");
    Await.result(offsetStore.createKeyspaceAndTable(), scala.concurrent.duration.Duration.create(10, TimeUnit.SECONDS));
  }

  @AfterClass
  public static void afterAll() throws Exception {
    session.executeDDL("DROP keyspace " + offsetStore.keyspace()).toCompletableFuture().get(10, TimeUnit.SECONDS);
  }

  static class Envelope {
    final String id;
    final long offset;
    final String message;

    Envelope(String id, long offset, String message) {
      this.id = id;
      this.offset = offset;
      this.message = message;
    }
  }

  static class TestSourceProvider extends SourceProvider<Long, Envelope> {

    private final String entityId;

    private final List<Envelope> envelopes;

    TestSourceProvider(String entityId) {
      this.entityId = entityId;
      envelopes = Arrays.asList(
          new Envelope(entityId, 1, "abc"),
          new Envelope(entityId, 2, "def"),
          new Envelope(entityId, 3, "ghi"),
          new Envelope(entityId, 4, "jkl"),
          new Envelope(entityId, 5, "mno"),
          new Envelope(entityId, 6, "pqr"));
    }

    @Override
    public CompletionStage<Source<Envelope, ?>> source(Supplier<CompletionStage<Optional<Long>>> offsetF) {
      return offsetF.get().toCompletableFuture().thenApplyAsync(offset -> {
        if (offset.isPresent())
          return Source.from(envelopes).drop(offset.get().intValue());
        else
          return Source.from(envelopes);
      });
    }

    @Override
    public Long extractOffset(Envelope envelope) {
      return envelope.offset;
    }
  }

  static class TestActorHandler extends ActorHandler<Envelope, TestHandlerBehavior.Req> {
    private final ActorSystem<?> system;
    private final Duration askTimeout = Duration.ofSeconds(5);

    public TestActorHandler(Behavior<TestHandlerBehavior.Req> behavior, ActorSystem<?> system) {
      super(behavior);
      this.system = system;
    }

    @Override
    public CompletionStage<Done> process(ActorRef<TestHandlerBehavior.Req> actor, Envelope envelope) {
      return AskPattern.ask(actor, (ActorRef<Done> replyTo) -> new TestHandlerBehavior.Req(envelope, replyTo),
          askTimeout, system.scheduler());
    }
  }

  static class TestHandlerBehavior {
    static class Req {
      public final Envelope envelope;
      public final ActorRef<Done> replyTo;

      Req(Envelope envelope, ActorRef<Done> replyTo) {
        this.envelope = envelope;
        this.replyTo = replyTo;
      }
    }

    static Behavior<Req> create(ActorRef<Envelope> receiveProbe, ActorRef<Done> stopProbe) {
      return Behaviors.receive(Req.class)
          .onMessage(Req.class, req -> {
            receiveProbe.tell(req.envelope);
            req.replyTo.tell(Done.getInstance());
            return Behaviors.same();
          })
          .onSignal(PostStop.class, postStop -> {
            stopProbe.tell(Done.getInstance());
            return Behaviors.same();
          })
          .build();
    }
  }

  private ProjectionTestKit projectionTestKit = ProjectionTestKit.create(testKit.testKit());

  private ProjectionId genRandomProjectionId() {
    return ProjectionId.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
  }

  scala.concurrent.duration.Duration awaitTimeout = scala.concurrent.duration.Duration.create(3, TimeUnit.SECONDS);

  private void assertStoredOffset(ProjectionId projectionId, long expectedOffset) {
    testKit.createTestProbe().awaitAssert(() -> {
      try {
        long offset = Await.result(offsetStore.<Long>readOffset(projectionId), awaitTimeout).get();
        assertEquals(expectedOffset, offset);
        return null;
      } catch (Exception e) {
        // from Await
        throw new RuntimeException(e);
      }
    });
  }

  private Handler<Envelope> concatHandler(StringBuffer str) {
    return Handler.fromFunction(envelope -> {
      str.append(envelope.message).append("|");
      return CompletableFuture.completedFuture(Done.getInstance());
    });
  }

  private Handler<Envelope> concatHandlerFail4(StringBuffer str) {
    return Handler.fromFunction(envelope -> {
      if (envelope.offset == 4)
        throw new RuntimeException("fail on 4");
      str.append(envelope.message).append("|");
      return CompletableFuture.completedFuture(Done.getInstance());
    });
  }

  static class GroupedConcatHandler extends Handler<List<Envelope>> {
    private final StringBuffer str;

    GroupedConcatHandler(StringBuffer str) {
      this.str = str;
    }

    @Override
    public CompletionStage<Done> process(List<Envelope> envelopes) {
      for (Envelope env : envelopes) {
        str.append(env.message).append("|");
      }
      return CompletableFuture.completedFuture(Done.getInstance());
    }
  }

  @Test
  public void atLeastOnceShouldStoreOffset() {
    String entityId = UUID.randomUUID().toString();
    ProjectionId projectionId = genRandomProjectionId();

    StringBuffer str = new StringBuffer();

    Projection<Envelope> projection = CassandraProjection
        .atLeastOnce(
            projectionId,
            new TestSourceProvider(entityId),
            () -> concatHandler(str))

        .withSaveOffset(1, Duration.ZERO);

    projectionTestKit.run(projection, () -> {
      assertEquals("abc|def|ghi|jkl|mno|pqr|", str.toString());
    });

    assertStoredOffset(projectionId, 6L);
  }

  @Test
  public void atLeastOnceShouldRestartFromPreviousOffset() {
    String entityId = UUID.randomUUID().toString();
    ProjectionId projectionId = genRandomProjectionId();

    StringBuffer str = new StringBuffer();

    Projection<Envelope> projection = CassandraProjection
        .atLeastOnce(
            projectionId,
            new TestSourceProvider(entityId),
            () -> concatHandlerFail4(str))
        .withSaveOffset(1, Duration.ZERO);

    try {
      projectionTestKit.run(projection, () -> {
        assertEquals("abc|def|ghi|", str.toString());
      });
      Assert.fail("Expected exception");
    } catch (RuntimeException e) {
      assertEquals("fail on 4", e.getMessage());
    }

    assertStoredOffset(projectionId, 3L);

    // re-run projection without failing function
    Projection<Envelope> projection2 = CassandraProjection
        .atLeastOnce(
            projectionId,
            new TestSourceProvider(entityId),
            () -> concatHandler(str))
        .withSaveOffset(1, Duration.ZERO);

    projectionTestKit.run(projection2, () -> {
      assertEquals("abc|def|ghi|jkl|mno|pqr|", str.toString());
    });
  }

  @Test
  public void groupedShouldStoreOffset() {
    String entityId = UUID.randomUUID().toString();
    ProjectionId projectionId = genRandomProjectionId();

    StringBuffer str = new StringBuffer();

    Projection<Envelope> projection = CassandraProjection
        .groupedWithin(
            projectionId,
            new TestSourceProvider(entityId),
            () -> new GroupedConcatHandler(str))
        .withGroup(3, Duration.ofMinutes(1));

    projectionTestKit.run(projection, () ->
        assertEquals("abc|def|ghi|jkl|mno|pqr|", str.toString()));

    assertStoredOffset(projectionId, 6L);
  }

  @Test
  public void atMostOnceShouldStoreOffset() {
    String entityId = UUID.randomUUID().toString();
    ProjectionId projectionId = genRandomProjectionId();

    StringBuffer str = new StringBuffer();

    Projection<Envelope> projection = CassandraProjection
        .atMostOnce(
            projectionId,
            new TestSourceProvider(entityId),
            () -> concatHandler(str));

    projectionTestKit.run(projection, () -> {
      assertEquals("abc|def|ghi|jkl|mno|pqr|", str.toString());
    });

    assertStoredOffset(projectionId, 6L);
  }

  @Test
  public void atMostOnceShouldRestartFromNextOffset() {
    String entityId = UUID.randomUUID().toString();
    ProjectionId projectionId = genRandomProjectionId();

    StringBuffer str = new StringBuffer();

    Projection<Envelope> projection = CassandraProjection
        .atMostOnce(
            projectionId,
            new TestSourceProvider(entityId),
            () -> concatHandlerFail4(str));

    try {
      projectionTestKit.run(projection, () -> {
        assertEquals("abc|def|ghi|", str.toString());
      });
      Assert.fail("Expected exception");
    } catch (RuntimeException e) {
      assertEquals("fail on 4", e.getMessage());
    }

    assertStoredOffset(projectionId, 4L);

    // re-run projection without failing function
    Projection<Envelope> projection2 = CassandraProjection
        .atMostOnce(
            projectionId,
            new TestSourceProvider(entityId),
            () -> concatHandler(str));

    projectionTestKit.run(projection2, () -> {
      // failed: jkl not included
      assertEquals("abc|def|ghi|mno|pqr|", str.toString());
    });
  }

  @Test
  public void actorHandlerShouldStartStopActor() {
    String entityId = UUID.randomUUID().toString();
    ProjectionId projectionId = genRandomProjectionId();

    TestProbe<Envelope> receiveProbe = testKit.createTestProbe();
    TestProbe<Done> stopProbe = testKit.createTestProbe();

    Projection<Envelope> projection = CassandraProjection
        .atLeastOnce(
            projectionId,
            new TestSourceProvider(entityId),
            () -> new TestActorHandler(TestHandlerBehavior.create(receiveProbe.getRef(), stopProbe.getRef()), testKit.system()))
        .withSaveOffset(1, Duration.ZERO);

    ActorRef<ProjectionBehavior.Command> projectionRef = testKit.spawn(ProjectionBehavior.create(projection));

    assertEquals("abc", receiveProbe.receiveMessage().message);
    assertEquals("def", receiveProbe.receiveMessage().message);
    assertEquals("ghi", receiveProbe.receiveMessage().message);
    assertEquals("jkl", receiveProbe.receiveMessage().message);
    assertEquals("mno", receiveProbe.receiveMessage().message);
    assertEquals("pqr", receiveProbe.receiveMessage().message);

    projectionRef.tell(ProjectionBehavior.stopMessage());

    stopProbe.receiveMessage();

    assertStoredOffset(projectionId, 6L);
  }


}
