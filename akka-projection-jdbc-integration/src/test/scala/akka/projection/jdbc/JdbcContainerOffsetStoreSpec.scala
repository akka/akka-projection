/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc

import java.sql.DriverManager

import scala.language.existentials

import akka.projection.TestTags
import akka.projection.jdbc.JdbcOffsetStoreSpec.JdbcSpecConfig
import akka.projection.jdbc.JdbcOffsetStoreSpec.PureJdbcSession
import akka.projection.jdbc.internal.Dialect
import akka.projection.jdbc.internal.OracleDialect
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.Tag
import org.testcontainers.containers._
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy

object JdbcContainerOffsetStoreSpec {

  abstract class ContainerJdbcSpecConfig(dialect: String) extends JdbcSpecConfig {

    val tag: Tag = TestTags.ContainerDb

    final val schemaName = "test_schema"

    override def config: Config =
      baseConfig.withFallback(ConfigFactory.parseString(s"""
        akka.projection.jdbc {
          offset-store.schema = "$schemaName"
          dialect = $dialect
        }
        """))

    def jdbcSessionFactory(): PureJdbcSession = {

      // this is safe as tests only start after the container is init
      val container = _container.get

      new PureJdbcSession(() => {
        Class.forName(container.getDriverClassName)
        val conn =
          DriverManager.getConnection(container.getJdbcUrl, container.getUsername, container.getPassword)
        conn.setAutoCommit(false)
        conn
      })
    }

    protected var _container: Option[JdbcDatabaseContainer[_]] = None

    def newContainer(): JdbcDatabaseContainer[_]

    final override def initContainer(): Unit = {
      val container = newContainer()
      _container = Some(container)
      container.withStartupCheckStrategy(new IsRunningStartupCheckStrategy)
      container.withStartupAttempts(5)
      container.start()
    }

    override def stopContainer(): Unit =
      _container.get.stop()
  }

  object PostgresSpecConfig extends ContainerJdbcSpecConfig("postgres-dialect") {
    val name = "Postgres Database"
    override def newContainer(): JdbcDatabaseContainer[_] =
      new PostgreSQLContainer("postgres:13.1").withInitScript("db/default-init.sql")
  }
  object PostgresLegacySchemaSpecConfig extends ContainerJdbcSpecConfig("postgres-dialect") {
    val name = "Postgres Database"

    override def config: Config =
      super.config.withFallback(ConfigFactory.parseString("""
        akka.projection.jdbc = {
           offset-store.use-lowercase-schema = false
        }
        """))

    override def newContainer(): JdbcDatabaseContainer[_] =
      new PostgreSQLContainer("postgres:13.1").withInitScript("db/default-init.sql")
  }

  object MySQLSpecConfig extends ContainerJdbcSpecConfig("mysql-dialect") {
    val name = "MySQL Database"
    override def newContainer(): JdbcDatabaseContainer[_] =
      new MySQLContainer("mysql:8.0.22").withDatabaseName(schemaName)
  }

  object MSSQLServerSpecConfig extends ContainerJdbcSpecConfig("mssql-dialect") {
    val name = "MS SQL Server Database"
    override val tag: Tag = TestTags.FlakyDb
    override def newContainer(): JdbcDatabaseContainer[_] =
      new MSSQLServerContainer("mcr.microsoft.com/mssql/server:2022-CU13-ubuntu-22.04")
        .withInitScript("db/default-init.sql")
  }

  object OracleSpecConfig extends ContainerJdbcSpecConfig("oracle-dialect") {
    val name = "Oracle Database"

    override def newContainer(): JdbcDatabaseContainer[_] = {
      // https://www.testcontainers.org/modules/databases/oraclexe/
      new OracleContainer("gvenzl/oracle-xe:21-slim-faststart")
        .withDatabaseName("TEST_SCHEMA")
        .withUsername("TEST_SCHEMA")
    }
  }
}

class PostgresJdbcOffsetStoreSpec extends JdbcOffsetStoreSpec(JdbcContainerOffsetStoreSpec.PostgresSpecConfig)

class PostgresJdbcOffsetStoreLegacySchemaSpec
    extends JdbcOffsetStoreSpec(JdbcContainerOffsetStoreSpec.PostgresLegacySchemaSpecConfig) {

  private val table = settings.schema.map(s => s""""$s"."${settings.table}"""").getOrElse(s""""${settings.table}"""")
  override def selectLastStatement: String =
    s"""SELECT * FROM $table WHERE "PROJECTION_NAME" = ? AND "PROJECTION_KEY" = ?"""
}

class MySQLJdbcOffsetStoreSpec extends JdbcOffsetStoreSpec(JdbcContainerOffsetStoreSpec.MySQLSpecConfig) {
  override def selectLastStatement: String = Dialect.removeQuotes(super.selectLastStatement)
}

class MSSQLServerJdbcOffsetStoreSpec extends JdbcOffsetStoreSpec(JdbcContainerOffsetStoreSpec.MSSQLServerSpecConfig)

class OracleJdbcOffsetStoreSpec extends JdbcOffsetStoreSpec(JdbcContainerOffsetStoreSpec.OracleSpecConfig) {

  private val dialect: Dialect = OracleDialect(settings.schema, settings.table, settings.managementTable)

  private val table =
    dialect.schema.map(s => s""""$s"."${dialect.tableName}"""").getOrElse(s""""${dialect.tableName}"""")
  override def selectLastStatement: String =
    s"""SELECT * FROM $table WHERE "PROJECTION_NAME" = ? AND "PROJECTION_KEY" = ?"""
}
