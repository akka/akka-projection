/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc

import java.sql.DriverManager

import akka.projection.TestTags
import akka.projection.jdbc.JdbcOffsetStoreSpec.{JdbcSpecConfig, PureJdbcSession}
import akka.projection.jdbc.internal.Dialect
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.Tag
import org.testcontainers.containers._
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy

import scala.language.existentials

object JdbcContainerOffsetStoreSpec {

  abstract class ContainerJdbcSpecConfig(dialect: String) extends JdbcSpecConfig {

    val tag: Tag = TestTags.ContainerDb

    final val schemaName = "test_schema"

    override val config: Config =
      baseConfig.withFallback(ConfigFactory.parseString(s"""
        akka.projection.jdbc = {
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
    override def newContainer(): JdbcDatabaseContainer[_] = new PostgreSQLContainer().withInitScript("db/default-init.sql")
  }

  object MySQLSpecConfig extends ContainerJdbcSpecConfig("mysql-dialect") {
    val name = "MySQL Database"
    override def newContainer(): JdbcDatabaseContainer[_] = new MySQLContainer().withDatabaseName(schemaName)
  }

  object MSSQLServerSpecConfig extends ContainerJdbcSpecConfig("mssql-dialect") {
    val name = "MS SQL Server Database"
    override val tag: Tag = TestTags.FlakyDb
    override def newContainer(): JdbcDatabaseContainer[_] = new MSSQLServerContainer().withInitScript("db/default-init.sql")
  }

  object OracleSpecConfig extends ContainerJdbcSpecConfig("oracle-dialect") {
    val name = "Oracle Database"
    override def newContainer() = new OracleContainer("oracleinanutshell/oracle-xe-11g").withInitScript("db/oracle-init.sql")
  }

}

class PostgresJdbcOffsetStoreSpec extends JdbcOffsetStoreSpec(JdbcContainerOffsetStoreSpec.PostgresSpecConfig)
class MySQLJdbcOffsetStoreSpec extends JdbcOffsetStoreSpec(JdbcContainerOffsetStoreSpec.MySQLSpecConfig) {
  override def selectLastStatement: String = Dialect.removeQuotes(super.selectLastStatement)
}
class MSSQLServerJdbcOffsetStoreSpec extends JdbcOffsetStoreSpec(JdbcContainerOffsetStoreSpec.MSSQLServerSpecConfig)
class OracleJdbcOffsetStoreSpec extends JdbcOffsetStoreSpec(JdbcContainerOffsetStoreSpec.OracleSpecConfig)
