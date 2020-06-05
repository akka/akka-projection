/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc

import java.sql.Connection
import java.sql.DriverManager
import java.time.Instant
import java.util.UUID

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.japi.function
import akka.persistence.query.Sequence
import akka.persistence.query.TimeBasedUUID
import akka.projection.ProjectionId
import akka.projection.jdbc.JdbcOffsetStoreSpec.JdbcSpecConfig
import akka.projection.jdbc.internal.JdbcOffsetStore
import akka.projection.jdbc.internal.JdbcSettings
import akka.projection.jdbc.javadsl.JdbcSession
import akka.projection.jdbc.javadsl.JdbcSession.tryWithResource
import akka.projection.jdbc.javadsl.JdbcSession.withConnection
import akka.projection.testkit.internal.TestClock
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
    val config: Config
    val jdbcSessionFactory: () => JdbcSession
  }

  private[akka] class PureJdbcSession(connFunc: () => Connection) extends JdbcSession {
    lazy val conn = connFunc()
    override def withConnection[Result](func: function.Function[Connection, Result]): Result =
      func(conn)

    override def commit(): Unit = conn.commit()

    override def rollback(): Unit = conn.rollback()

    override def close(): Unit = conn.close()
  }

  object H2SpecConfig extends JdbcSpecConfig {

    val name = "H2 Database"
    val config: Config = ConfigFactory.parseString("""
    akka.projection.jdbc = {
      dialect = "h2-dialect"
      offset-store {
        schema = ""
        table = "AKKA_PROJECTION_OFFSET_STORE"
      }
      
      # TODO: configure a connection pool for the tests
      blocking-jdbc-dispatcher.thread-pool-executor.fixed-pool-size = 5
    }
    """)

    val jdbcSessionFactory = () =>
      new PureJdbcSession(() => {
        Class.forName("org.h2.Driver")
        val c = DriverManager.getConnection("jdbc:h2:mem:offset-store-test;DB_CLOSE_DELAY=-1")
        c.setAutoCommit(false)
        c
      })
  }
}

abstract class JdbcOffsetStoreSpec(specConfig: JdbcSpecConfig)
    extends ScalaTestWithActorTestKit(specConfig.config)
    with AnyWordSpecLike
    with LogCapturing
    with OptionValues {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(3, Seconds), interval = Span(100, Millis))

  implicit val executionContext = testKit.system.executionContext

  // test clock for testing of the `last_updated` Instant
  private val clock = new TestClock

  private val settings = JdbcSettings(testKit.system)
  private val offsetStore = new JdbcOffsetStore(settings, specConfig.jdbcSessionFactory, clock)
  private val dialectLabel = specConfig.name

  override protected def beforeAll(): Unit = {
    // create offset table
    Await.result(offsetStore.createIfNotExists(), 3.seconds)
  }

  override protected def afterAll(): Unit = {}

  private def selectLastUpdated(projectionId: ProjectionId): Instant = {
    withConnection(specConfig.jdbcSessionFactory) { conn =>
      val statement = s"SELECT * FROM ${settings.table} WHERE projection_name = ? AND projection_key = ?"
      tryWithResource(conn.prepareStatement(statement)) { stmt =>
        stmt.setString(1, projectionId.name)
        stmt.setString(2, projectionId.key)
        val resultSet = stmt.executeQuery()

        if (resultSet.first()) {
          val t = resultSet.getTimestamp(6)
          Instant.ofEpochMilli(t.getTime)
        } else throw new RuntimeException(s"no records found for $projectionId")
      }
    }.futureValue
  }

  s"The JdbcOffsetStore [$dialectLabel]" must {

    "create and update offsets" in {

      val projectionId = ProjectionId("projection-with-long", "00")

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

      val projectionId = ProjectionId("projection-with-long", "00")

      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, 1L).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[Long](projectionId)
        offset.futureValue.value shouldBe 1L
      }

    }

    "save and retrieve offsets of type java.lang.Long" in {

      val projectionId = ProjectionId("projection-with-java-long", "00")
      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, java.lang.Long.valueOf(1L)).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[java.lang.Long](projectionId)
        offset.futureValue.value shouldBe 1L
      }
    }

    "save and retrieve offsets of type Int" in {

      val projectionId = ProjectionId("projection-with-int", "00")
      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, 1).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[Int](projectionId)
        offset.futureValue.value shouldBe 1
      }

    }

    "save and retrieve offsets of type java.lang.Integer" in {

      val projectionId = ProjectionId("projection-with-java-int", "00")

      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, java.lang.Integer.valueOf(1)).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[java.lang.Integer](projectionId)
        offset.futureValue.value shouldBe 1
      }
    }

    "save and retrieve offsets of type String" in {

      val projectionId = ProjectionId("projection-with-String", "00")
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

      val projectionId = ProjectionId("projection-with-akka-seq", "00")

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

      val projectionId = ProjectionId("projection-with-akka-seq", "00")

      val timeOffset = TimeBasedUUID(UUID.fromString("49225740-2019-11ea-a752-ffae2393b6e4")) //2019-12-16T15:32:36.148Z[UTC]
      withClue("check - save offset") {
        offsetStore.saveOffset(projectionId, timeOffset).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[TimeBasedUUID](projectionId).futureValue.value
        offset shouldBe timeOffset
      }
    }

    "update timestamp" in {
      val projectionId = ProjectionId("timestamp", "00")

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
      val projectionId = ProjectionId("projection-clear", "00")

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

class H2JdbcOffsetStoreSpec extends JdbcOffsetStoreSpec(JdbcOffsetStoreSpec.H2SpecConfig)
