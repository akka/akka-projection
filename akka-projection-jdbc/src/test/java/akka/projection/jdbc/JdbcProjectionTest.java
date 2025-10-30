/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.jdbc;

import akka.Done;
import akka.NotUsed;
import akka.actor.testkit.typed.javadsl.LogCapturing;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.japi.function.Function;
import akka.japi.pf.Match;
import akka.projection.Projection;
import akka.projection.ProjectionContext;
import akka.projection.ProjectionId;
import akka.projection.javadsl.SourceProvider;
import akka.projection.jdbc.internal.JdbcOffsetStore;
import akka.projection.jdbc.internal.JdbcSettings;
import akka.projection.jdbc.javadsl.JdbcHandler;
import akka.projection.jdbc.javadsl.JdbcProjection;
import akka.projection.testkit.javadsl.ProjectionTestKit;
import akka.projection.testkit.javadsl.TestSourceProvider;
import akka.stream.javadsl.FlowWithContext;
import akka.stream.javadsl.Source;
import akka.stream.testkit.TestSubscriber;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;
import scala.Option;
import scala.PartialFunction;
import scala.concurrent.Await;
import scala.concurrent.Future;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;

public class JdbcProjectionTest extends JUnitSuite {

  private static final Map<String, Object> configuration = new HashMap<>();

  static {
    configuration.put("akka.projection.jdbc.dialect", "h2-dialect");
    configuration.put("akka.projection.jdbc.offset-store.schema", "");
    configuration.put("akka.projection.jdbc.offset-store.table", "akka_projection_offset_store");
    configuration.put("akka.projection.jdbc.use-dispatcher", "database.dispatcher");
    configuration.put("database.dispatcher.executor", "thread-pool-executor");
    configuration.put("database.dispatcher.throughput", 1);
    configuration.put("database.dispatcher.thread-pool-executor.fixed-pool-size", 5);
  }

  private static final Config config = ConfigFactory.parseMap(configuration);

  @ClassRule public static final TestKitJunitResource testKit = new TestKitJunitResource(config);

  @Rule public final LogCapturing logCapturing = new LogCapturing();

  static class PureJdbcSession implements JdbcSession {

    private final Connection connection;

    public PureJdbcSession() {
      try {
        Class.forName("org.h2.Driver");
        Connection c = DriverManager.getConnection("jdbc:h2:mem:test-java;DB_CLOSE_DELAY=-1;OPTIMIZE_REUSE_RESULTS=FALSE");
        c.setAutoCommit(false);
        this.connection = c;
      } catch (ClassNotFoundException | SQLException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public <Result> Result withConnection(Function<Connection, Result> func) throws Exception {
      return func.apply(connection);
    }

    @Override
    public void commit() throws SQLException {
      connection.commit();
    }

    @Override
    public void rollback() throws SQLException {
      connection.rollback();
    }

    @Override
    public void close() throws SQLException {
      connection.close();
    }
  }

  private static class JdbcSessionCreator implements Supplier<PureJdbcSession> {

    @Override
    public PureJdbcSession get() {
      return new PureJdbcSession();
    }
  }

  private static final JdbcSessionCreator jdbcSessionCreator = new JdbcSessionCreator();

  private static final JdbcSettings jdbcSettings = JdbcSettings.apply(testKit.system());
  private static final JdbcOffsetStore<PureJdbcSession> offsetStore =
      new JdbcOffsetStore<>(testKit.system(), jdbcSettings, jdbcSessionCreator::get);

  private static final scala.concurrent.duration.Duration awaitTimeout =
      scala.concurrent.duration.Duration.create(3, TimeUnit.SECONDS);

  @BeforeClass
  public static void beforeAll() throws Exception {
    Await.result(offsetStore.createIfNotExists(), awaitTimeout);
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
    Source<Envelope, NotUsed> envelopes =
        Source.from(
            Arrays.asList(
                new Envelope(entityId, 1, "abc"),
                new Envelope(entityId, 2, "def"),
                new Envelope(entityId, 3, "ghi"),
                new Envelope(entityId, 4, "jkl"),
                new Envelope(entityId, 5, "mno"),
                new Envelope(entityId, 6, "pqr")));

    TestSourceProvider<Long, Envelope> sourceProvider =
        TestSourceProvider.create(envelopes, env -> env.offset)
            .withStartSourceFrom(
                (Long lastProcessedOffset, Long offset) -> offset <= lastProcessedOffset);

    return sourceProvider;
  }

  private final ProjectionTestKit projectionTestKit = ProjectionTestKit.create(testKit.system());

  private ProjectionId genRandomProjectionId() {
    return ProjectionId.of(UUID.randomUUID().toString(), "00");
  }

  private void assertStoredOffset(ProjectionId projectionId, long expectedOffset) {
    testKit
        .createTestProbe()
        .awaitAssert(
            () -> {
              try {
                Future<Option<Long>> futOffset = offsetStore.readOffset(projectionId);
                long offset = Await.result(futOffset, awaitTimeout).get();
                assertEquals(expectedOffset, offset);
                return null;
              } catch (Exception e) {
                // from Await
                throw new RuntimeException(e);
              }
            });
  }

  private String failMessage(long offset) {
    return "fail on envelope with offset: [" + offset + "]";
  }

  private void expectNextUntilErrorMessage(TestSubscriber.Probe<Done> probe, String msg) {
    probe.request(1);
    PartialFunction<TestSubscriber.SubscriberEvent, Boolean> pf =
        Match.<TestSubscriber.SubscriberEvent, Boolean, TestSubscriber.OnError>match(
                TestSubscriber.OnError.class,
                err -> err.cause().getMessage().equals(msg),
                event -> true)
            .match(TestSubscriber.OnNext.class, event -> false)
            .build();
    if (!probe.expectEventPF(pf)) expectNextUntilErrorMessage(probe, msg);
  }

  private JdbcHandler<Envelope, PureJdbcSession> concatHandler(StringBuffer str) {
    return concatHandler(str, __ -> false);
  }

  private JdbcHandler<Envelope, PureJdbcSession> concatHandler(
      StringBuffer buffer, Predicate<Long> failPredicate) {
    return JdbcHandler.fromFunction(
        (PureJdbcSession session, Envelope envelope) -> {
          if (failPredicate.test(envelope.offset)) {
            throw new RuntimeException(failMessage(envelope.offset));
          } else {
            buffer.append(envelope.message).append("|");
          }
        });
  }

  GroupedConcatHandler groupedConcatHandler(StringBuffer buffer, TestProbe<String> handlerProbe) {
    return new GroupedConcatHandler(buffer, handlerProbe);
  }

  static class GroupedConcatHandler extends JdbcHandler<List<Envelope>, PureJdbcSession> {

    public static final String handlerCalled = "called";
    private final StringBuffer buffer;
    private final TestProbe<String> handlerProbe;

    GroupedConcatHandler(StringBuffer buffer, TestProbe<String> handlerProbe) {
      this.buffer = buffer;
      this.handlerProbe = handlerProbe;
    }

    @Override
    public void process(PureJdbcSession session, List<Envelope> envelopes) {
      handlerProbe.ref().tell(GroupedConcatHandler.handlerCalled);
      for (Envelope envelope : envelopes) {
        buffer.append(envelope.message).append("|");
      }
    }
  }

  @Test
  public void exactlyOnceShouldStoreOffset() {
    String entityId = UUID.randomUUID().toString();
    ProjectionId projectionId = genRandomProjectionId();

    StringBuffer str = new StringBuffer();

    Projection<Envelope> projection =
        JdbcProjection.exactlyOnce(
            projectionId,
            sourceProvider(entityId),
            jdbcSessionCreator,
            () -> concatHandler(str),
            testKit.system());

    projectionTestKit.run(
        projection, () -> assertEquals("abc|def|ghi|jkl|mno|pqr|", str.toString()));

    assertStoredOffset(projectionId, 6L);
  }

  @Test
  public void exactlyOnceShouldRestartFromPreviousOffset() {
    String entityId = UUID.randomUUID().toString();
    ProjectionId projectionId = genRandomProjectionId();

    StringBuffer str = new StringBuffer();

    Projection<Envelope> projection =
        JdbcProjection.exactlyOnce(
            projectionId,
            sourceProvider(entityId),
            jdbcSessionCreator,
            // fail on fourth offset
            () -> concatHandler(str, offset -> offset == 4),
            testKit.system());

    projectionTestKit.runWithTestSink(
        projection,
        (probe) -> {
          probe.request(3);
          probe.expectNextN(3);
          assertEquals("abc|def|ghi|", str.toString());
          expectNextUntilErrorMessage(probe, failMessage(4));
        });
  }

  @Test
  public void atLeastOnceShouldStoreOffset() {
    String entityId = UUID.randomUUID().toString();
    ProjectionId projectionId = genRandomProjectionId();

    StringBuffer str = new StringBuffer();

    Projection<Envelope> projection =
        JdbcProjection.atLeastOnce(
                projectionId,
                sourceProvider(entityId),
                jdbcSessionCreator,
                () -> concatHandler(str),
                testKit.system())
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
        JdbcProjection.atLeastOnce(
                projectionId,
                sourceProvider(entityId),
                jdbcSessionCreator,
                // fail on fourth offset
                () -> concatHandler(str, offset -> offset == 4),
                testKit.system())
            .withSaveOffset(1, Duration.ZERO);

    projectionTestKit.runWithTestSink(
        projection,
        (probe) -> {
          probe.request(3);
          testKit.createTestProbe().awaitAssert(() -> {
            assertEquals("abc|def|ghi|", str.toString());
            return null;
          });
        });

    assertStoredOffset(projectionId, 3L);

    // re-run projection without failing function
    Projection<Envelope> projection2 =
        JdbcProjection.atLeastOnce(
                projectionId,
                sourceProvider(entityId),
                jdbcSessionCreator,
                () -> concatHandler(str),
                testKit.system())
            .withSaveOffset(1, Duration.ZERO);

    projectionTestKit.run(
        projection2,
        () -> {
          assertEquals("abc|def|ghi|jkl|mno|pqr|", str.toString());
        });
    assertStoredOffset(projectionId, 6L);
  }

  @Test
  public void groupedShouldStoreOffset() {

    String entityId = UUID.randomUUID().toString();
    ProjectionId projectionId = genRandomProjectionId();

    TestProbe<String> handlerProbe = testKit.createTestProbe("calls-to-handler");
    StringBuffer str = new StringBuffer();

    Projection<Envelope> projection =
        JdbcProjection.groupedWithin(
                projectionId,
                sourceProvider(entityId),
                jdbcSessionCreator,
                () -> groupedConcatHandler(str, handlerProbe),
                testKit.system())
            .withGroup(3, Duration.ofMinutes(1));

    projectionTestKit.run(
        projection, () -> assertEquals("abc|def|ghi|jkl|mno|pqr|", str.toString()));

    assertStoredOffset(projectionId, 6L);

    // handler probe is called twice
    handlerProbe.expectMessage(GroupedConcatHandler.handlerCalled);
    handlerProbe.expectMessage(GroupedConcatHandler.handlerCalled);
  }

  @Test
  public void atLeastOnceFlowShouldStoreOffset() {

    String entityId = UUID.randomUUID().toString();
    ProjectionId projectionId = genRandomProjectionId();

    StringBuffer str = new StringBuffer();

    FlowWithContext<Envelope, ProjectionContext, Done, ProjectionContext, NotUsed> flow =
        FlowWithContext.<Envelope, ProjectionContext>create()
            .map(
                envelope -> {
                  str.append(envelope.message).append("|");
                  return Done.getInstance();
                });

    Projection<Envelope> projection =
        JdbcProjection.atLeastOnceFlow(
                projectionId, sourceProvider(entityId), jdbcSessionCreator, flow, testKit.system())
            .withSaveOffset(1, Duration.ofMinutes(1));

    projectionTestKit.run(
        projection, () -> assertEquals("abc|def|ghi|jkl|mno|pqr|", str.toString()));

    assertStoredOffset(projectionId, 6L);
  }
}
