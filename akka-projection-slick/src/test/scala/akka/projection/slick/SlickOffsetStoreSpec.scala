/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick

import java.time.Instant
import java.util.UUID

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.query.Sequence
import akka.persistence.query.TimeBasedUUID
import akka.projection.MergeableOffset
import akka.projection.ProjectionId
import akka.projection.StringKey
import akka.projection.TestTags
import akka.projection.slick.SlickOffsetStoreSpec.SlickSpecConfig
import akka.projection.slick.internal.SlickOffsetStore
import akka.projection.slick.internal.SlickSettings
import akka.projection.testkit.internal.TestClock
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.OptionValues
import org.scalatest.Tag
import org.scalatest.time.Millis
import org.scalatest.time.Seconds
import org.scalatest.time.Span
import org.scalatest.wordspec.AnyWordSpecLike
import org.testcontainers.containers.JdbcDatabaseContainer
import org.testcontainers.containers.MSSQLServerContainer
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.OracleContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile

object SlickOffsetStoreSpec {

  trait SlickSpecConfig {
    val name: String
    def tag: Tag
    val baseConfig = ConfigFactory.parseString("""
    akka.projection.slick = {
      offset-store {
        schema = ""
        table = "AKKA_PROJECTION_OFFSET_STORE"
      }
    }
    """)
    def config: Config
    def stopContainer(): Unit

  }

  object H2SpecConfig extends SlickSpecConfig {

    val name = "H2 Database"
    val tag = TestTags.InMemoryDb
    override def config: Config =
      baseConfig.withFallback(ConfigFactory.parseString("""
        akka.projection.slick = {
           profile = "slick.jdbc.H2Profile$"
           db = {
             url = "jdbc:h2:mem:offset-store-test-slick;DB_CLOSE_DELAY=-1"
             driver = org.h2.Driver
             connectionPool = disabled
             keepAliveConnection = true
           }
        }
        """))

    override def stopContainer(): Unit = ()
  }

  abstract class ContainerJdbcSpecConfig extends SlickSpecConfig {

    val tag: Tag = TestTags.ContainerDb
    def container: JdbcDatabaseContainer[_]

    override def config = {
      baseConfig.withFallback(ConfigFactory.parseString(s"""
        akka.projection.slick = {
           db = {
             url = "${container.getJdbcUrl}"
             driver = ${container.getDriverClassName}
             user = ${container.getUsername}
             password = ${container.getPassword}
             connectionPool = disabled
             keepAliveConnection = true
           }
        }
        """))

    }

    protected def initContainer(container: JdbcDatabaseContainer[_]): JdbcDatabaseContainer[_] = {
      container.withStartupCheckStrategy(new IsRunningStartupCheckStrategy)
      container.withStartupAttempts(5)
      container.start()
      container
    }

    override def stopContainer(): Unit =
      container.stop()
  }

  class PostgresSpecConfig extends ContainerJdbcSpecConfig {

    val name = "Postgres Database"
    val container = initContainer(new PostgreSQLContainer)

    override def config: Config =
      super.config.withFallback(ConfigFactory.parseString("""
        akka.projection.slick = {
           profile = "slick.jdbc.PostgresProfile$"
        }
        """))
  }

  class MySQLSpecConfig extends ContainerJdbcSpecConfig {

    val name = "MySQL Database"
    val container = initContainer(new MySQLContainer)

    override def config: Config =
      super.config.withFallback(ConfigFactory.parseString("""
        akka.projection.slick = {
           profile = "slick.jdbc.MySQLProfile$"
        }
        """))
  }
  class MSSQLServerSpecConfig extends ContainerJdbcSpecConfig {

    val name = "MS SQL Server Database"
    override val tag = TestTags.FlakyDb

    val container = initContainer(new MSSQLServerContainer)

    override def config: Config =
      super.config.withFallback(ConfigFactory.parseString("""
        akka.projection.slick = {
           profile = "slick.jdbc.SQLServerProfile$"
        }
        """))
  }
  class OracleSpecConfig extends ContainerJdbcSpecConfig {

    val name = "Oracle Database"
    val container = initContainer(new OracleContainer("oracleinanutshell/oracle-xe-11g"))

    override def config: Config =
      super.config.withFallback(ConfigFactory.parseString("""
        akka.projection.slick = {
           profile = "slick.jdbc.OracleProfile$"
        }
        """))
  }
}
class H2SlickOffsetStoreSpec extends SlickOffsetStoreSpec(SlickOffsetStoreSpec.H2SpecConfig)
class PostgresSlickOffsetStoreSpec extends SlickOffsetStoreSpec(new SlickOffsetStoreSpec.PostgresSpecConfig)
class MySQLSlickOffsetStoreSpec extends SlickOffsetStoreSpec(new SlickOffsetStoreSpec.MySQLSpecConfig)
class MSSQLServerSlickOffsetStoreSpec extends SlickOffsetStoreSpec(new SlickOffsetStoreSpec.MSSQLServerSpecConfig)
class OracleSlickOffsetStoreSpec extends SlickOffsetStoreSpec(new SlickOffsetStoreSpec.OracleSpecConfig)

abstract class SlickOffsetStoreSpec(specConfig: SlickSpecConfig)
    extends ScalaTestWithActorTestKit(specConfig.config)
    with LogCapturing
    with AnyWordSpecLike
    with OptionValues {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(3, Seconds), interval = Span(100, Millis))

  private val slickConfig = specConfig.config.getConfig(SlickSettings.configPath)
  private val dialectLabel = specConfig.name

  val dbConfig: DatabaseConfig[JdbcProfile] =
    DatabaseConfig.forConfig(SlickSettings.configPath, specConfig.config)

  // test clock for testing of the `last_updated` Instant
  private val clock = new TestClock

  private val offsetStore =
    new SlickOffsetStore(system, dbConfig.db, dbConfig.profile, SlickSettings(slickConfig), clock)

  override protected def beforeAll(): Unit = {
    // create offset table
    Await.result(offsetStore.createIfNotExists, 30.seconds)
  }

  override protected def afterAll(): Unit = {
    dbConfig.db.close()
    specConfig.stopContainer()
  }

  private def selectLastUpdated(projectionId: ProjectionId): Instant = {
    import dbConfig.profile.api._
    val action = offsetStore.offsetTable
      .filter(r => r.projectionName === projectionId.name && r.projectionKey === projectionId.key)
      .result
      .headOption
    dbConfig.db.run(action).futureValue.get.lastUpdated
  }

  private def genRandomProjectionId() = ProjectionId(UUID.randomUUID().toString, "00")

  "The SlickOffsetStore" must {

    implicit val ec: ExecutionContext = dbConfig.db.executor.executionContext

    s"create and update offsets [$dialectLabel]" taggedAs (specConfig.tag) in {

      val projectionId = genRandomProjectionId()

      withClue("check - save offset 1L") {
        dbConfig.db.run(offsetStore.saveOffset(projectionId, 1L)).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 1L
      }

      withClue("check - save offset 2L") {
        dbConfig.db.run(offsetStore.saveOffset(projectionId, 2L)).futureValue
      }

      withClue("check - read offset after overwrite") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 2L // yep, saveOffset overwrites previous
      }

    }

    s"save and retrieve offsets of type Long [$dialectLabel]" taggedAs (specConfig.tag) in {

      val projectionId = genRandomProjectionId()

      withClue("check - save offset") {
        dbConfig.db.run(offsetStore.saveOffset(projectionId, 1L)).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 1L
      }

    }

    s"save and retrieve offsets of type java.lang.Long [$dialectLabel]" taggedAs (specConfig.tag) in {

      val projectionId = genRandomProjectionId()

      withClue("check - save offset") {
        dbConfig.db.run(offsetStore.saveOffset(projectionId, java.lang.Long.valueOf(1L))).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[java.lang.Long](projectionId).futureValue.value
        offset shouldBe 1L
      }
    }

    s"save and retrieve offsets of type Int [$dialectLabel]" taggedAs (specConfig.tag) in {

      val projectionId = genRandomProjectionId()

      withClue("check - save offset") {
        dbConfig.db.run(offsetStore.saveOffset(projectionId, 1)).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[Int](projectionId).futureValue.value
        offset shouldBe 1
      }

    }

    s"save and retrieve offsets of type java.lang.Integer [$dialectLabel]" taggedAs (specConfig.tag) in {

      val projectionId = genRandomProjectionId()

      withClue("check - save offset") {
        dbConfig.db.run(offsetStore.saveOffset(projectionId, java.lang.Integer.valueOf(1))).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[java.lang.Integer](projectionId).futureValue.value
        offset shouldBe 1
      }
    }

    s"save and retrieve offsets of type String [$dialectLabel]" taggedAs (specConfig.tag) in {

      val projectionId = genRandomProjectionId()

      val randOffset = UUID.randomUUID().toString
      withClue("check - save offset") {
        dbConfig.db.run(offsetStore.saveOffset(projectionId, randOffset)).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[String](projectionId).futureValue.value
        offset shouldBe randOffset
      }
    }

    s"save and retrieve offsets of type akka.persistence.query.Sequence [$dialectLabel]" taggedAs (specConfig.tag) in {

      val projectionId = genRandomProjectionId()

      val seqOffset = Sequence(1L)
      withClue("check - save offset") {
        dbConfig.db.run(offsetStore.saveOffset(projectionId, seqOffset)).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[Sequence](projectionId).futureValue.value
        offset shouldBe seqOffset
      }
    }

    s"save and retrieve offsets of type akka.persistence.query.TimeBasedUUID [$dialectLabel]" taggedAs (specConfig.tag) in {

      val projectionId = genRandomProjectionId()

      val timeOffset = TimeBasedUUID(UUID.fromString("49225740-2019-11ea-a752-ffae2393b6e4")) //2019-12-16T15:32:36.148Z[UTC]
      withClue("check - save offset") {
        dbConfig.db.run(offsetStore.saveOffset(projectionId, timeOffset)).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[TimeBasedUUID](projectionId).futureValue.value
        offset shouldBe timeOffset
      }
    }

    s"update timestamp [$dialectLabel]" taggedAs (specConfig.tag) in {
      val projectionId = genRandomProjectionId()

      val instant0 = clock.instant()
      dbConfig.db.run(offsetStore.saveOffset(projectionId, 15)).futureValue
      val instant1 = selectLastUpdated(projectionId)
      instant1 shouldBe instant0

      val instant2 = clock.tick(java.time.Duration.ofMillis(5))
      dbConfig.db.run(offsetStore.saveOffset(projectionId, 16)).futureValue
      val instant3 = selectLastUpdated(projectionId)
      instant3 shouldBe instant2
    }

    s"save and retrieve MergeableOffset [$dialectLabel]" taggedAs (specConfig.tag) in {

      val projectionId = genRandomProjectionId()

      val origOffset = MergeableOffset(Map(StringKey("abc") -> 1L, StringKey("def") -> 1L, StringKey("ghi") -> 1L))
      withClue("check - save offset") {
        dbConfig.db.run(offsetStore.saveOffset(projectionId, origOffset)).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[MergeableOffset[StringKey, Long]](projectionId)
        offset.futureValue.value shouldBe origOffset
      }
    }

    s"add new offsets to MergeableOffset [$dialectLabel]" taggedAs (specConfig.tag) in {

      val projectionId = genRandomProjectionId()

      val origOffset = MergeableOffset(Map(StringKey("abc") -> 1L, StringKey("def") -> 1L))
      withClue("check - save offset") {
        dbConfig.db.run(offsetStore.saveOffset(projectionId, origOffset)).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[MergeableOffset[StringKey, Long]](projectionId)
        offset.futureValue.value shouldBe origOffset
      }

      // mix updates and inserts
      val updatedOffset = MergeableOffset(Map(StringKey("abc") -> 2L, StringKey("def") -> 2L, StringKey("ghi") -> 1L))
      withClue("check - save offset") {
        dbConfig.db.run(offsetStore.saveOffset(projectionId, updatedOffset)).futureValue
      }

      withClue("check - read offset") {
        val offset = offsetStore.readOffset[MergeableOffset[StringKey, Long]](projectionId)
        offset.futureValue.value shouldBe updatedOffset
      }
    }

    s"clear offset [$dialectLabel]" taggedAs (specConfig.tag) in {
      val projectionId = genRandomProjectionId()

      withClue("check - save offset") {
        dbConfig.db.run(offsetStore.saveOffset(projectionId, 3L)).futureValue
      }

      withClue("check - read offset") {
        offsetStore.readOffset[Long](projectionId).futureValue shouldBe Some(3L)
      }

      withClue("check - clear offset") {
        dbConfig.db.run(offsetStore.clearOffset(projectionId)).futureValue
      }

      withClue("check - read offset") {
        offsetStore.readOffset[Long](projectionId).futureValue shouldBe None
      }
    }
  }
}
