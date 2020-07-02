/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.jdbc;

import akka.NotUsed;
import akka.actor.testkit.typed.javadsl.LogCapturing;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.japi.function.Creator;
import akka.projection.Projection;
import akka.projection.ProjectionId;
import akka.projection.javadsl.SourceProvider;
import akka.projection.jdbc.JdbcProjectionTest;
import akka.projection.jdbc.internal.JdbcOffsetStore;
import akka.projection.jdbc.internal.JdbcSettings;
import akka.projection.jdbc.javadsl.JdbcHandler;
import akka.projection.jdbc.javadsl.JdbcProjection;
import akka.projection.testkit.javadsl.ProjectionTestKit;
import akka.stream.javadsl.Source;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;
import scala.Option;
import scala.concurrent.Await;
import scala.concurrent.Future;

import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;

public class JdbcHibernateTest extends JUnitSuite {
  private static final Map<String, Object> configuration = new HashMap<>();

  static {
    configuration.put("akka.projection.jdbc.dialect", "h2-dialect");
    configuration.put("akka.projection.jdbc.offset-store.schema", "");
    configuration.put("akka.projection.jdbc.offset-store.table", "AKKA_PROJECTION_OFFSET_STORE");
    configuration.put(
        "akka.projection.jdbc.blocking-jdbc-dispatcher.thread-pool-executor.fixed-pool-size", 5);
  }

  private static final Config config = ConfigFactory.parseMap(configuration);
  @ClassRule public static final TestKitJunitResource testKit = new TestKitJunitResource(config);
  @Rule public final LogCapturing logCapturing = new LogCapturing();

  private final ProjectionTestKit projectionTestKit = ProjectionTestKit.create(testKit.testKit());

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

  private static final HibernateSessionFactory sessionProvider = new HibernateSessionFactory();

  private static final JdbcSettings jdbcSettings = JdbcSettings.apply(testKit.system());
  private static final JdbcOffsetStore<HibernateJdbcSession> offsetStore =
      new JdbcOffsetStore<>(testKit.system(), jdbcSettings, () -> sessionProvider.newInstance());

  private static final scala.concurrent.duration.Duration awaitTimeout =
      scala.concurrent.duration.Duration.create(3, TimeUnit.SECONDS);

  @BeforeClass
  public static void beforeAll() throws Exception {
    Await.result(offsetStore.createIfNotExists(), awaitTimeout);
  }

  static class TestSourceProvider extends SourceProvider<Long, Envelope> {

    private final List<Envelope> envelopes;

    TestSourceProvider(String entityId) {
      envelopes =
          Arrays.asList(
              new Envelope(entityId, 1, "abc"),
              new Envelope(entityId, 2, "def"),
              new Envelope(entityId, 3, "ghi"),
              new Envelope(entityId, 4, "jkl"),
              new Envelope(entityId, 5, "mno"),
              new Envelope(entityId, 6, "pqr"));
    }

    @Override
    public CompletionStage<Source<Envelope, NotUsed>> source(
        Supplier<CompletionStage<Optional<Long>>> offsetF) {
      return offsetF
          .get()
          .toCompletableFuture()
          .thenApplyAsync(
              offset -> {
                if (offset.isPresent()) return Source.from(envelopes).drop(offset.get().intValue());
                else return Source.from(envelopes);
              });
    }

    @Override
    public Long extractOffset(Envelope envelope) {
      return envelope.offset;
    }
  }

  private JdbcHandler<Envelope, HibernateJdbcSession> concatHandler(StringBuffer buffer) {
    return new JdbcHandler<Envelope, HibernateJdbcSession>() {
      @Override
      public void process(HibernateJdbcSession session, Envelope envelope) {
        buffer.append(envelope.message).append("|");
      }
    };
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

  @Test
  public void testWeCanEffectivelyIntegrateWithHibernate() {

    String entityId = UUID.randomUUID().toString();
    ProjectionId projectionId = ProjectionId.of(UUID.randomUUID().toString(), "00");
    StringBuffer buffer = new StringBuffer();

    Projection<Envelope> projection =
        JdbcProjection.exactlyOnce(
            projectionId,
            new TestSourceProvider(entityId),
            () -> sessionProvider.newInstance(),
            () -> concatHandler(buffer),
            testKit.system());

    projectionTestKit.run(
        projection, () -> assertEquals("abc|def|ghi|jkl|mno|pqr|", buffer.toString()));

    assertStoredOffset(projectionId, 6L);
  }
}
