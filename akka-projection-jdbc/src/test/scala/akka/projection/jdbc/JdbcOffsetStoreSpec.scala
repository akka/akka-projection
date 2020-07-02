/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc

import java.sql.Connection
import java.sql.DriverManager
import java.time.Instant
import java.util.UUID

import scala.concurrent.Await
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.japi.function
import akka.persistence.query.Sequence
import akka.projection.MergeableOffset
import akka.projection.ProjectionId
import akka.projection.StringKey
import akka.projection.jdbc.JdbcOffsetStoreSpec.JdbcSpecConfig
import akka.projection.jdbc.JdbcOffsetStoreSpec.MySQLSpecConfig
import akka.projection.jdbc.internal.Dialect
import akka.projection.jdbc.internal.JdbcOffsetStore
import akka.projection.jdbc.internal.JdbcSessionUtil.tryWithResource
import akka.projection.jdbc.internal.JdbcSessionUtil.withConnection
import akka.projection.jdbc.internal.JdbcSettings
import akka.projection.testkit.internal.TestClock
import com.dimafeng.testcontainers.JdbcDatabaseContainer
import com.dimafeng.testcontainers.MSSQLServerContainer
import com.dimafeng.testcontainers.MySQLContainer
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.SingleContainer
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.OptionValues
import org.scalatest.time.Millis
import org.scalatest.time.Seconds
import org.scalatest.time.Span
import org.scalatest.wordspec.AnyWordSpecLike

object JdbcOffsetStoreSpec {

  trait JdbcSpecConfig {
    val name: String

    val config: Config = ConfigFactory.parseString("""
    
    akka.projection.jdbc = {
      offset-store {
        schema = ""
        table = "AKKA_PROJECTION_OFFSET_STORE"
      }
      
      # TODO: configure a connection pool for the tests
      blocking-jdbc-dispatcher.thread-pool-executor.fixed-pool-size = 5
    }
    """)
    def jdbcSessionFactory(): JdbcSession

    def initContainer(): Unit
    def stopContainer(): Unit
  }

  private[projection] class PureJdbcSession(connFunc: () => Connection) extends JdbcSession {

    lazy val conn = connFunc()
    override def withConnection[Result](func: function.Function[Connection, Result]): Result =
      func(conn)

    override def commit(): Unit = conn.commit()

    override def rollback(): Unit = conn.rollback()

    override def close(): Unit = conn.close()
  }

  object H2SpecConfig extends JdbcSpecConfig {

    val name = "H2 Database"
    override val config: Config =
      ConfigFactory.parseString("""
        akka.projection.jdbc = {
          dialect = "h2-dialect"
          offset-store {
            schema = ""
            table = "AKKA_PROJECTION_OFFSET_STORE"
          }
          
          blocking-jdbc-dispatcher.thread-pool-executor.fixed-pool-size = 5
        }
        """)

    def jdbcSessionFactory(): PureJdbcSession =
      new PureJdbcSession(() => {
        Class.forName("org.h2.Driver")
        val conn = DriverManager.getConnection("jdbc:h2:mem:offset-store-test-jdbc;DB_CLOSE_DELAY=-1")
        conn.setAutoCommit(false)
        conn
      })

    override def initContainer() = ()

    override def stopContainer() = ()
  }

  abstract class ContainerJdbcSpecConfig(dialect: String) extends JdbcSpecConfig {

    override val config: Config =
      ConfigFactory.parseString(s"""
        akka.projection.jdbc = {
          dialect = $dialect
          offset-store {
            schema = ""
            table = "AKKA_PROJECTION_OFFSET_STORE"
          }
          
          blocking-jdbc-dispatcher.thread-pool-executor.fixed-pool-size = 5
        }
        """)

    def jdbcSessionFactory(): PureJdbcSession = {

      // this is safe as tests only start after the container is init
      val container = _container.get

      new PureJdbcSession(() => {
        Class.forName(container.driverClassName)
        val conn =
          DriverManager.getConnection(container.jdbcUrl, container.username, container.password)
        conn.setAutoCommit(false)
        conn
      })
    }

    protected var _container: Option[JdbcDatabaseContainer] = None

    override def stopContainer(): Unit =
      _container.get.asInstanceOf[SingleContainer[_]].stop()
  }

  object PostgresSpecConfig extends ContainerJdbcSpecConfig("postgres-dialect") {

    val name = "Postgres Database"

    override def initContainer(): Unit = {
      val container = new PostgreSQLContainer
      _container = Some(container)
      container.start()
    }
  }

  object MySQLSpecConfig extends ContainerJdbcSpecConfig("mysql-dialect") {

    val name = "MySQL Database"

    override def initContainer(): Unit = {
      val container = new MySQLContainer
      _container = Some(container)
      container.start()
    }
  }

  object MSSQLServerSpecConfig extends ContainerJdbcSpecConfig("mssql-dialect") {

    val name = "MS SQL Server Database"

    override def initContainer(): Unit = {
      val container = new MSSQLServerContainer
      _container = Some(container)
      container.start()
    }
  }

}

class H2JdbcOffsetStoreSpec extends JdbcOffsetStoreSpec(JdbcOffsetStoreSpec.H2SpecConfig)
class PostgresJdbcOffsetStoreSpec extends JdbcOffsetStoreSpec(JdbcOffsetStoreSpec.PostgresSpecConfig)
class MySQLJdbcOffsetStoreSpec extends JdbcOffsetStoreSpec(JdbcOffsetStoreSpec.MySQLSpecConfig)
class MSSQLServerJdbcOffsetStoreSpec extends JdbcOffsetStoreSpec(JdbcOffsetStoreSpec.MSSQLServerSpecConfig)

abstract class JdbcOffsetStoreSpec(specConfig: JdbcSpecConfig)
    extends ScalaTestWithActorTestKit(specConfig.config)
    with AnyWordSpecLike
    with LogCapturing
    with OptionValues {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(3, Seconds), interval = Span(100, Millis))

  implicit val executionContext: ExecutionContextExecutor = testKit.system.executionContext

  // test clock for testing of the `last_updated` Instant
  private val clock = new TestClock

  private val settings = JdbcSettings(testKit.system)
  private val offsetStore = new JdbcOffsetStore(system, settings, specConfig.jdbcSessionFactory _, clock)
  private val dialectLabel = specConfig.name

  override protected def beforeAll(): Unit = {
    // start test container if needed
    // Note, the H2 test don't run in container and are therefore will run must faster
    specConfig.initContainer()

    // create offset table
    Await.result(offsetStore.createIfNotExists(), 3.seconds)
  }

  override protected def afterAll(): Unit =
    specConfig.stopContainer()

  private def selectLastUpdated(projectionId: ProjectionId): Instant = {
    withConnection(specConfig.jdbcSessionFactory _) { conn =>

      val statement = {
        val stmt = s"""SELECT * FROM "${settings.table}" WHERE "PROJECTION_NAME" = ? AND "PROJECTION_KEY" = ?"""
        specConfig match {
          case MySQLSpecConfig => Dialect.removeQuotes(stmt)
          case _               => stmt
        }
      }

      // init statement in try-with-resource
      tryWithResource(conn.prepareStatement(statement)) { stmt =>
        stmt.setString(1, projectionId.name)
        stmt.setString(2, projectionId.key)

        // init ResultSet in try-with-resource
        tryWithResource(stmt.executeQuery()) { resultSet =>

          if (resultSet.next()) {
            val t = resultSet.getTimestamp(6)
            Instant.ofEpochMilli(t.getTime)
          } else throw new RuntimeException(s"no records found for $projectionId")
        }
      }
    }.futureValue
  }

  private def genRandomProjectionId() = ProjectionId(UUID.randomUUID().toString, "00")

  s"The JdbcOffsetStore [$dialectLabel]" must {

    "create and update offsets" in {

      val projectionId = genRandomProjectionId()

      withClue("check - save offset 1L") {
        offsetStore.saveOffset(projectionId, 1L).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[Long](projectionId)
        offset.futureValue.value shouldBe 1L
      }

      withClue("check - save offset 2L") {
        offsetStore.saveOffset(projectionId, 2L).futureValue
      }

      withClue("check - read offset after overwrite") {
        val offset = offsetStore.readOffset[Long](projectionId)
        offset.futureValue.value shouldBe 2L // yep, saveOffset overwrites previous
      }

    }

    "save and retrieve offsets of type Long" in {

      val projectionId = genRandomProjectionId()

      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, 1L).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[Long](projectionId)
        offset.futureValue.value shouldBe 1L
      }

    }

    "save and retrieve offsets of type java.lang.Long" in {

      val projectionId = genRandomProjectionId()
      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, java.lang.Long.valueOf(1L)).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[java.lang.Long](projectionId)
        offset.futureValue.value shouldBe 1L
      }
    }

    "save and retrieve offsets of type Int" in {

      val projectionId = genRandomProjectionId()
      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, 1).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[Int](projectionId)
        offset.futureValue.value shouldBe 1
      }

    }

    "save and retrieve offsets of type java.lang.Integer" in {

      val projectionId = genRandomProjectionId()

      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, java.lang.Integer.valueOf(1)).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[java.lang.Integer](projectionId)
        offset.futureValue.value shouldBe 1
      }
    }

    "save and retrieve offsets of type String" in {

      val projectionId = genRandomProjectionId()
      val randOffset = UUID.randomUUID().toString
      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, randOffset).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[String](projectionId)
        offset.futureValue.value shouldBe randOffset
      }
    }

    "save and retrieve offsets of type akka.persistence.query.Sequence" in {

      val projectionId = genRandomProjectionId()

      val seqOffset = Sequence(1L)
      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, seqOffset).futureValue
      }

      withClue("check - read offset") {
        val offset =
          offsetStore.readOffset[Sequence](projectionId).futureValue.value
        offset shouldBe seqOffset
      }
    }

    "save and retrieve offsets of type akka.persistence.query.TimeBasedUUID" in {

      val projectionId = genRandomProjectionId()

      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, 1L).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[Long](projectionId)
        offset.futureValue.value shouldBe 1L
      }
    }

    "save and retrieve MergeableOffset" in {

      val projectionId = genRandomProjectionId()

      val origOffset = MergeableOffset(Map(StringKey("abc") -> 1L, StringKey("def") -> 1L, StringKey("ghi") -> 1L))
      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, origOffset).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[MergeableOffset[StringKey, Long]](projectionId)
        offset.futureValue.value shouldBe origOffset
      }
    }

    "add new offsets to MergeableOffset" in {

      val projectionId = genRandomProjectionId()

      val origOffset = MergeableOffset(Map(StringKey("abc") -> 1L, StringKey("def") -> 1L))
      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, origOffset).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[MergeableOffset[StringKey, Long]](projectionId)
        offset.futureValue.value shouldBe origOffset
      }

      // mix updates and inserts
      val updatedOffset = MergeableOffset(Map(StringKey("abc") -> 2L, StringKey("def") -> 2L, StringKey("ghi") -> 1L))
      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, updatedOffset).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[MergeableOffset[StringKey, Long]](projectionId)
        offset.futureValue.value shouldBe updatedOffset
      }
    }

    "update timestamp" in {

      val projectionId = genRandomProjectionId()

      val instant0 = clock.instant()
      offsetStore.saveOffset(projectionId, 15).futureValue

      val instant1 = selectLastUpdated(projectionId)
      instant1 shouldBe instant0

      val instant2 = clock.tick(java.time.Duration.ofMillis(5))
      offsetStore.saveOffset(projectionId, 16).futureValue

      val instant3 = selectLastUpdated(projectionId)
      instant3 shouldBe instant2
    }

    "clear offset" in {

      val projectionId = genRandomProjectionId()

      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, 3L).futureValue
      }

      withClue("check - read offset") {
        offsetStore.readOffset[Long](projectionId).futureValue shouldBe Some(3L)
      }

      withClue("check - clear offset") {
        offsetStore.clearOffset(projectionId).futureValue
      }

      withClue("check - read offset") {
        offsetStore.readOffset[Long](projectionId).futureValue shouldBe None
      }
    }
  }
}
