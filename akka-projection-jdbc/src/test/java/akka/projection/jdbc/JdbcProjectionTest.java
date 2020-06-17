/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc;


import akka.actor.testkit.typed.javadsl.LogCapturing;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.japi.function.Creator;
import akka.japi.function.Function;
import akka.projection.Projection;
import akka.projection.ProjectionId;
import akka.projection.javadsl.SourceProvider;
import akka.projection.jdbc.internal.JdbcOffsetStore;
import akka.projection.jdbc.internal.JdbcSettings;
import akka.projection.jdbc.javadsl.JdbcHandler;
import akka.projection.jdbc.javadsl.JdbcProjection;
import akka.projection.testkit.javadsl.ProjectionTestKit;
import akka.stream.javadsl.Source;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.*;
import org.scalatestplus.junit.JUnitSuite;
import scala.Option;
import scala.concurrent.Await;
import scala.concurrent.Future;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;

public class JdbcProjectionTest extends JUnitSuite {


    private static final Map<String, Object> configuration = new HashMap<>();

    static {
        configuration.put("akka.projection.jdbc.dialect", "h2-dialect");
        configuration.put("akka.projection.jdbc.offset-store.schema", "");
        configuration.put("akka.projection.jdbc.offset-store.table", "AKKA_PROJECTION_OFFSET_STORE");
        configuration.put("akka.projection.jdbc.blocking-jdbc-dispatcher.thread-pool-executor.fixed-pool-size", 5);
    }

    private static final Config config = ConfigFactory.parseMap(configuration);

    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource(config);

    @Rule
    public final LogCapturing logCapturing = new LogCapturing();
    static class PureJdbcSession implements JdbcSession {

        private final Connection connection;

        public PureJdbcSession() {
            try {
                Class.forName("org.h2.Driver");
                Connection c = DriverManager.getConnection("jdbc:h2:mem:test-java;DB_CLOSE_DELAY=-1");
                c.setAutoCommit(false);
                this.connection = c ;
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


    private static class JdbcSessionCreator implements Creator<PureJdbcSession> {

        @Override
        public PureJdbcSession create()  {
                return new PureJdbcSession();
        }
    }

    private static final JdbcSessionCreator jdbcSessionCreator = new JdbcSessionCreator();


    private static final JdbcSettings jdbcSettings = JdbcSettings.apply(testKit.system());
    private static final JdbcOffsetStore<PureJdbcSession> offsetStore = new JdbcOffsetStore<>(jdbcSettings, jdbcSessionCreator::create);

    private static final
    scala.concurrent.duration.Duration awaitTimeout = scala.concurrent.duration.Duration.create(3, TimeUnit.SECONDS);

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

    static class TestSourceProvider extends SourceProvider<Long, Envelope> {


        private final List<Envelope> envelopes;

        TestSourceProvider(String entityId) {
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



    private final ProjectionTestKit projectionTestKit = ProjectionTestKit.create(testKit.testKit());

    private ProjectionId genRandomProjectionId() {
        return ProjectionId.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    }

    private void assertStoredOffset(ProjectionId projectionId, long expectedOffset) {
        testKit.createTestProbe().awaitAssert(() -> {
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

    private JdbcHandler<Envelope, PureJdbcSession> concatHandler(StringBuffer str, Predicate<Long> failPredicate) {
        return new JdbcHandler<Envelope, PureJdbcSession>() {
            @Override
            public void process(PureJdbcSession session, Envelope envelope) {
                if (failPredicate.test(envelope.offset)) {
                    throw new RuntimeException(failMessage(envelope.offset));
                } else {
                    str.append(envelope.message).append("|");
                }
            }
        };
    }

    private JdbcHandler<Envelope, PureJdbcSession> concatHandler(StringBuffer str) {
        return concatHandler(str, __ -> false);
    }


    @Test
    public void exactlyOnceShouldStoreOffset() {
        String entityId = UUID.randomUUID().toString();
        ProjectionId projectionId = genRandomProjectionId();

        StringBuffer str = new StringBuffer();

        Projection<Envelope> projection =
            JdbcProjection
              .exactlyOnce(
                  projectionId,
                  new TestSourceProvider(entityId),
                  jdbcSessionCreator,
                  concatHandler(str),
                  testKit.system()
              );

        projectionTestKit.run(projection, () -> assertEquals("abc|def|ghi|jkl|mno|pqr|", str.toString()));

        assertStoredOffset(projectionId, 6L);
    }

    @Test
    public void exactlyOnceShouldRestartFromPreviousOffset() {
        String entityId = UUID.randomUUID().toString();
        ProjectionId projectionId = genRandomProjectionId();

        StringBuffer str = new StringBuffer();

        Projection<Envelope> projection =
            JdbcProjection
                .exactlyOnce(
                    projectionId,
                    new TestSourceProvider(entityId),
                    jdbcSessionCreator,
                    // fail on forth offset
                    concatHandler(str, offset -> offset == 4),
                    testKit.system()
                );

        try {
            projectionTestKit.run(projection, () -> assertEquals("abc|def|ghi|", str.toString()));
            Assert.fail("Expected exception");
        } catch (RuntimeException e) {
            assertEquals(failMessage(4), e.getMessage());
        }

        assertStoredOffset(projectionId, 3L);

        // re-run projection without failing function
        Projection<Envelope> projection2 =
            JdbcProjection
                .exactlyOnce(
                    projectionId,
                    new TestSourceProvider(entityId),
                    jdbcSessionCreator,
                    // fail on forth offset
                    concatHandler(str),
                    testKit.system()
                );
        projectionTestKit.run(projection2, () -> assertEquals("abc|def|ghi|jkl|mno|pqr|", str.toString()));

        assertStoredOffset(projectionId, 6L);
    }



}
