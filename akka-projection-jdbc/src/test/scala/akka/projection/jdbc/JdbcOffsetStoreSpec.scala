/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc

import java.sql.Connection
import java.sql.DriverManager
import java.time.Instant
import java.util.UUID

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.Future
import scala.util.Try

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.japi.function
import akka.persistence.query.Sequence
import akka.projection.jdbc.JdbcOffsetStoreSpec.JdbcSpecConfig
import akka.projection.jdbc.internal.JdbcSessionUtil.tryWithResource
import akka.projection.jdbc.internal.JdbcSessionUtil.withConnection
import akka.projection.jdbc.internal.JdbcOffsetStore
import akka.projection.jdbc.internal.JdbcSettings
import akka.projection.testkit.internal.TestClock
import akka.projection.MergeableOffset
import akka.projection.ProjectionId
import akka.projection.TestTags
import akka.projection.internal.ManagementState
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.OptionValues
import org.scalatest.Tag

object JdbcOffsetStoreSpec {

  trait JdbcSpecConfig {
    val name: String
    def tag: Tag
    val baseConfig = ConfigFactory.parseString("""
    
    akka {
      loglevel = "DEBUG"
      projection.jdbc {
        offset-store {
          table = "akka_projection_offset_store"
          with-lowercase-schema = true
        }
        
        blocking-jdbc-dispatcher.thread-pool-executor.fixed-pool-size = 5
        debug.verbose-offset-store-logging = true
      }
    }
    """)
    def config: Config
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
    val tag: Tag = TestTags.InMemoryDb

    override val config: Config =
      baseConfig.withFallback(ConfigFactory.parseString("""
        akka.projection.jdbc {
          dialect = "h2-dialect"
        }
        """))

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
}

class H2JdbcOffsetStoreSpec extends JdbcOffsetStoreSpec(JdbcOffsetStoreSpec.H2SpecConfig)

abstract class JdbcOffsetStoreSpec(specConfig: JdbcSpecConfig)
    extends ScalaTestWithActorTestKit(specConfig.config)
    with AnyWordSpecLike
    with LogCapturing
    with OptionValues {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = 10.seconds, interval = 100.millis)

  private implicit val executionContext: ExecutionContextExecutor = system.executionContext
  private implicit val classicScheduler = system.classicSystem.scheduler

  // test clock for testing of the `last_updated` Instant
  private val clock = new TestClock

  val settings = JdbcSettings(testKit.system)
  private val offsetStore = new JdbcOffsetStore(system, settings, specConfig.jdbcSessionFactory _, clock)
  private val dialectLabel = specConfig.name

  override protected def beforeAll(): Unit = {
    // start test container if needed
    // Note, the H2 test don't run in container and are therefore will run must faster
    // wrapping Future to at least be able to add a timeout
    Await.result(Future.fromTry(Try(specConfig.initContainer())), 30.seconds)

    // create offset table
    // the container can takes time to be 'ready',
    // we should keep trying to create the table until it succeeds
    Await.result(akka.pattern.retry(() => offsetStore.createIfNotExists(), 20, 3.seconds), 60.seconds)
  }

  override protected def afterAll(): Unit =
    specConfig.stopContainer()

  private val table = settings.schema.map(s => s""""$s"."${settings.table}"""").getOrElse(s""""${settings.table}"""")

  def selectLastStatement: String = {
    // wrapping with quotes always work as long as case is respected
    s"""SELECT * FROM $table WHERE "projection_name" = ? AND "projection_key" = ?"""
  }

  private def selectLastUpdated(projectionId: ProjectionId): Instant = {
    withConnection(specConfig.jdbcSessionFactory _) { conn =>

      val statement = selectLastStatement

      // init statement in try-with-resource
      tryWithResource(conn.prepareStatement(statement)) { stmt =>
        stmt.setString(1, projectionId.name)
        stmt.setString(2, projectionId.key)

        // init ResultSet in try-with-resource
        tryWithResource(stmt.executeQuery()) { resultSet =>

          if (resultSet.next()) {
            val millisSinceEpoch = resultSet.getLong(6)
            Instant.ofEpochMilli(millisSinceEpoch)
          } else throw new RuntimeException(s"no records found for $projectionId")
        }
      }
    }.futureValue
  }

  private def genRandomProjectionId() = ProjectionId(UUID.randomUUID().toString, "00")

  "The JdbcOffsetStore" must {

    s"not fail when dropIfExists and createIfNotExists are called [$dialectLabel]" in {
      // this is already called on setup, should not fail if called again
      val dropAndCreate =
        for {
          _ <- offsetStore.dropIfExists()
          _ <- offsetStore.createIfNotExists()
        } yield Done
      dropAndCreate.futureValue
    }

    s"create and update offsets [$dialectLabel]" taggedAs (specConfig.tag) in {

      val projectionId = genRandomProjectionId()

      withClue("check - save offset 1L") {
        offsetStore.saveOffset(projectionId, 1L).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[Long](projectionId)
        offset.futureValue shouldBe Some(1L)
      }

      withClue("check - save offset 2L") {
        offsetStore.saveOffset(projectionId, 2L).futureValue
      }

      withClue("check - read offset after overwrite") {
        val offset = offsetStore.readOffset[Long](projectionId)
        offset.futureValue shouldBe Some(2L) // yep, saveOffset overwrites previous
      }

    }

    s"save and retrieve offsets of type Long [$dialectLabel]" taggedAs (specConfig.tag) in {

      val projectionId = genRandomProjectionId()

      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, 1L).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[Long](projectionId)
        offset.futureValue shouldBe Some(1L)
      }

    }

    s"save and retrieve offsets of type java.lang.Long [$dialectLabel]" taggedAs (specConfig.tag) in {

      val projectionId = genRandomProjectionId()
      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, java.lang.Long.valueOf(1L)).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[java.lang.Long](projectionId)
        offset.futureValue shouldBe Some(1L)
      }
    }

    s"save and retrieve offsets of type Int [$dialectLabel]" taggedAs (specConfig.tag) in {

      val projectionId = genRandomProjectionId()
      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, 1).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[Int](projectionId)
        offset.futureValue shouldBe Some(1)
      }

    }

    s"save and retrieve offsets of type java.lang.Integer [$dialectLabel]" taggedAs (specConfig.tag) in {

      val projectionId = genRandomProjectionId()

      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, java.lang.Integer.valueOf(1)).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[java.lang.Integer](projectionId)
        offset.futureValue shouldBe Some(1)
      }
    }

    s"save and retrieve offsets of type String [$dialectLabel]" taggedAs (specConfig.tag) in {

      val projectionId = genRandomProjectionId()
      val randOffset = UUID.randomUUID().toString
      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, randOffset).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[String](projectionId)
        offset.futureValue shouldBe Some(randOffset)
      }
    }

    s"save and retrieve offsets of type akka.persistence.query.Sequence [$dialectLabel]" taggedAs (specConfig.tag) in {

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

    s"save and retrieve offsets of type akka.persistence.query.TimeBasedUUID [$dialectLabel]" taggedAs (specConfig.tag) in {

      val projectionId = genRandomProjectionId()

      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, 1L).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[Long](projectionId)
        offset.futureValue shouldBe Some(1L)
      }
    }

    s"save and retrieve MergeableOffset [$dialectLabel]" taggedAs (specConfig.tag) in {

      val projectionId = genRandomProjectionId()

      val origOffset = MergeableOffset(Map("abc" -> 1L, "def" -> 1L, "ghi" -> 1L))
      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, origOffset).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[MergeableOffset[Long]](projectionId)
        offset.futureValue shouldBe Some(origOffset)
      }
    }

    s"add new offsets to MergeableOffset [$dialectLabel]" taggedAs (specConfig.tag) in {

      val projectionId = genRandomProjectionId()

      val origOffset = MergeableOffset(Map("abc" -> 1L, "def" -> 1L))
      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, origOffset).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[MergeableOffset[Long]](projectionId)
        offset.futureValue shouldBe Some(origOffset)
      }

      // mix updates and inserts
      val updatedOffset = MergeableOffset(Map("abc" -> 2L, "def" -> 2L, "ghi" -> 1L))
      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, updatedOffset).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[MergeableOffset[Long]](projectionId)
        offset.futureValue shouldBe Some(updatedOffset)
      }
    }

    s"update timestamp [$dialectLabel]" taggedAs (specConfig.tag) in {

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

    s"clear offset [$dialectLabel]" taggedAs (specConfig.tag) in {

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

    s"read and save paused [$dialectLabel]" taggedAs (specConfig.tag) in {
      val projectionId = genRandomProjectionId()

      offsetStore.readManagementState(projectionId).futureValue shouldBe None

      offsetStore.savePaused(projectionId, paused = true).futureValue
      offsetStore.readManagementState(projectionId).futureValue shouldBe Some(ManagementState(paused = true))

      offsetStore.savePaused(projectionId, paused = false).futureValue
      offsetStore.readManagementState(projectionId).futureValue shouldBe Some(ManagementState(paused = false))
    }
  }
}
