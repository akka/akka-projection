/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra;

import akka.Done;
import akka.NotUsed;
import akka.actor.testkit.typed.javadsl.LogCapturing;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.javadsl.Behaviors;
import akka.pattern.Patterns;
import akka.projection.Projection;
import akka.projection.ProjectionBehavior;
import akka.projection.ProjectionId;
import akka.projection.cassandra.internal.CassandraOffsetStore;
import akka.projection.cassandra.javadsl.CassandraProjection;
import akka.projection.javadsl.ActorHandler;
import akka.projection.javadsl.Handler;
import akka.projection.javadsl.SourceProvider;
import akka.projection.testkit.javadsl.TestSourceProvider;
import akka.projection.testkit.javadsl.ProjectionTestKit;
import akka.stream.alpakka.cassandra.javadsl.CassandraSession;
import akka.stream.alpakka.cassandra.javadsl.CassandraSessionRegistry;
import akka.stream.javadsl.Source;
import org.junit.*;
import org.scalatestplus.junit.JUnitSuite;
import scala.compat.java8.FutureConverters;
import scala.concurrent.Await;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class CassandraProjectionTest extends JUnitSuite {
  @ClassRule public static final TestKitJunitResource testKit = new TestKitJunitResource();

  @Rule public final LogCapturing logCapturing = new LogCapturing();

  private static CassandraSession session;
  private static CassandraOffsetStore offsetStore;

  @BeforeClass
  public static void beforeAll() throws Exception {

    // don't use futureValue (patience) here because it can take a while to start the test container
    Await.result(
        ContainerSessionProvider.started(),
        scala.concurrent.duration.Duration.create(30, TimeUnit.SECONDS));

    offsetStore = new CassandraOffsetStore(testKit.system());
    session =
        CassandraSessionRegistry.get(testKit.system())
            .sessionFor("akka.projection.cassandra.session-config");

    // the container can takes time to be 'ready',
    // we should keep trying to create the table until it succeeds
    CompletionStage<Done> createTableAttempts =
        Patterns.retry(
            () -> FutureConverters.toJava(offsetStore.createKeyspaceAndTable()),
            20,
            Duration.ofSeconds(3),
            testKit.system().classicSystem().scheduler(),
            testKit.system().executionContext());
    Await.result(
        FutureConverters.toScala(createTableAttempts),
        scala.concurrent.duration.Duration.create(60, TimeUnit.SECONDS));
  }

  @AfterClass
  public static void afterAll() throws Exception {
    session
        .executeDDL("DROP keyspace " + offsetStore.keyspace())
        .toCompletableFuture()
        .get(10, TimeUnit.SECONDS);
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

  public static SourceProvider<Long, Envelope> sourceProvider(String entityId) {
    Source<Envelope, NotUsed> envelopes = Source.from(Arrays.asList(
      new Envelope(entityId, 1, "abc"),
      new Envelope(entityId, 2, "def"),
      new Envelope(entityId, 3, "ghi"),
      new Envelope(entityId, 4, "jkl"),
      new Envelope(entityId, 5, "mno"),
      new Envelope(entityId, 6, "pqr")));

    TestSourceProvider<Long, Envelope> sourceProvider = TestSourceProvider.create(envelopes, env -> env.offset)
      .withStartSourceFrom((Long lastProcessedOffset, Long offset) -> offset <= lastProcessedOffset);

    return sourceProvider;
  }

  static class TestActorHandler extends ActorHandler<Envelope, TestHandlerBehavior.Req> {
    private final ActorSystem<?> system;
    private final Duration askTimeout = Duration.ofSeconds(5);

    public TestActorHandler(Behavior<TestHandlerBehavior.Req> behavior, ActorSystem<?> system) {
      super(behavior);
      this.system = system;
    }

    @Override
    public CompletionStage<Done> process(
        ActorRef<TestHandlerBehavior.Req> actor, Envelope envelope) {
      return AskPattern.ask(
          actor,
          (ActorRef<Done> replyTo) -> new TestHandlerBehavior.Req(envelope, replyTo),
          askTimeout,
          system.scheduler());
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
          .onMessage(
              Req.class,
              req -> {
                receiveProbe.tell(req.envelope);
                req.replyTo.tell(Done.getInstance());
                return Behaviors.same();
              })
          .onSignal(
              PostStop.class,
              postStop -> {
                stopProbe.tell(Done.getInstance());
                return Behaviors.same();
              })
          .build();
    }
  }

  private ProjectionTestKit projectionTestKit = ProjectionTestKit.create(testKit.system());

  private ProjectionId genRandomProjectionId() {
    return ProjectionId.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
  }

  scala.concurrent.duration.Duration awaitTimeout =
      scala.concurrent.duration.Duration.create(3, TimeUnit.SECONDS);

  private void assertStoredOffset(ProjectionId projectionId, long expectedOffset) {
    testKit
        .createTestProbe()
        .awaitAssert(
            () -> {
              try {
                long offset =
                    Await.result(offsetStore.<Long>readOffset(projectionId), awaitTimeout).get();
                assertEquals(expectedOffset, offset);
                return null;
              } catch (Exception e) {
                // from Await
                throw new RuntimeException(e);
              }
            });
  }

  private Handler<Envelope> concatHandler(StringBuffer str) {
    return Handler.fromFunction(
        envelope -> {
          str.append(envelope.message).append("|");
          return CompletableFuture.completedFuture(Done.getInstance());
        });
  }

  private Handler<Envelope> concatHandlerFail4(StringBuffer str) {
    return Handler.fromFunction(
        envelope -> {
          if (envelope.offset == 4) throw new RuntimeException("fail on 4");
          str.append(envelope.message).append("|");
          return CompletableFuture.completedFuture(Done.getInstance());
        });
  }

  static class GroupedConcatHandler extends Handler<List<Envelope>> {

    public static final String handlerCalled = "called";
    private final StringBuffer str;
    private final TestProbe<String> handlerProbe;

    GroupedConcatHandler(StringBuffer buffer, TestProbe<String> handlerProbe) {
      this.str = buffer;
      this.handlerProbe = handlerProbe;
    }

    @Override
    public CompletionStage<Done> process(List<Envelope> envelopes) {
      handlerProbe.ref().tell(GroupedConcatHandler.handlerCalled);
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

    Projection<Envelope> projection =
        CassandraProjection.atLeastOnce(
                projectionId, sourceProvider(entityId), () -> concatHandler(str))
            .withSaveOffset(1, Duration.ZERO);

    projectionTestKit.run(
        projection,
        () -> {
          assertEquals("abc|def|ghi|jkl|mno|pqr|", str.toString());
        });

    assertStoredOffset(projectionId, 6L);
  }

  @Test
  public void atLeastOnceShouldRestartFromPreviousOffset() {
    String entityId = UUID.randomUUID().toString();
    ProjectionId projectionId = genRandomProjectionId();

    StringBuffer str = new StringBuffer();

    Projection<Envelope> projection =
        CassandraProjection.atLeastOnce(
                projectionId, sourceProvider(entityId), () -> concatHandlerFail4(str))
            .withSaveOffset(1, Duration.ZERO);

    try {
      projectionTestKit.run(
          projection,
          () -> {
            assertEquals("abc|def|ghi|", str.toString());
          });
      Assert.fail("Expected exception");
    } catch (RuntimeException e) {
      assertEquals("fail on 4", e.getMessage());
    }

    assertStoredOffset(projectionId, 3L);

    // re-run projection without failing function
    Projection<Envelope> projection2 =
        CassandraProjection.atLeastOnce(
                projectionId, sourceProvider(entityId), () -> concatHandler(str))
            .withSaveOffset(1, Duration.ZERO);

    projectionTestKit.run(
        projection2,
        () -> {
          assertEquals("abc|def|ghi|jkl|mno|pqr|", str.toString());
        });
  }

  @Test
  public void groupedShouldStoreOffset() {
    String entityId = UUID.randomUUID().toString();
    ProjectionId projectionId = genRandomProjectionId();

    StringBuffer str = new StringBuffer();

    TestProbe<String> handlerProbe = testKit.createTestProbe("calls-to-handler");
    Projection<Envelope> projection =
        CassandraProjection.groupedWithin(
                projectionId,
                sourceProvider(entityId),
                () -> new GroupedConcatHandler(str, handlerProbe))
            .withGroup(3, Duration.ofMinutes(1));

    projectionTestKit.run(
        projection, () -> assertEquals("abc|def|ghi|jkl|mno|pqr|", str.toString()));

    assertStoredOffset(projectionId, 6L);

    // handler probe is called twice
    handlerProbe.expectMessage(GroupedConcatHandler.handlerCalled);
    handlerProbe.expectMessage(GroupedConcatHandler.handlerCalled);
  }

  @Test
  public void atMostOnceShouldStoreOffset() {
    String entityId = UUID.randomUUID().toString();
    ProjectionId projectionId = genRandomProjectionId();

    StringBuffer str = new StringBuffer();

    Projection<Envelope> projection =
        CassandraProjection.atMostOnce(
            projectionId, sourceProvider(entityId), () -> concatHandler(str));

    projectionTestKit.run(
        projection,
        () -> {
          assertEquals("abc|def|ghi|jkl|mno|pqr|", str.toString());
        });

    assertStoredOffset(projectionId, 6L);
  }

  @Test
  public void atMostOnceShouldRestartFromNextOffset() {
    String entityId = UUID.randomUUID().toString();
    ProjectionId projectionId = genRandomProjectionId();

    StringBuffer str = new StringBuffer();

    Projection<Envelope> projection =
        CassandraProjection.atMostOnce(
            projectionId, sourceProvider(entityId), () -> concatHandlerFail4(str));

    try {
      projectionTestKit.run(
          projection,
          () -> {
            assertEquals("abc|def|ghi|", str.toString());
          });
      Assert.fail("Expected exception");
    } catch (RuntimeException e) {
      assertEquals("fail on 4", e.getMessage());
    }

    assertStoredOffset(projectionId, 4L);

    // re-run projection without failing function
    Projection<Envelope> projection2 =
        CassandraProjection.atMostOnce(
            projectionId, sourceProvider(entityId), () -> concatHandler(str));

    projectionTestKit.run(
        projection2,
        () -> {
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

    Projection<Envelope> projection =
        CassandraProjection.atLeastOnce(
                projectionId,
                sourceProvider(entityId),
                () ->
                    new TestActorHandler(
                        TestHandlerBehavior.create(receiveProbe.getRef(), stopProbe.getRef()),
                        testKit.system()))
            .withSaveOffset(1, Duration.ZERO);

    ActorRef<ProjectionBehavior.Command> projectionRef =
        testKit.spawn(ProjectionBehavior.create(projection));

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
