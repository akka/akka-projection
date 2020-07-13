/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc

import java.sql.DriverManager

import scala.language.existentials

import akka.projection.TestTags
import akka.projection.jdbc.JdbcOffsetStoreSpec.JdbcSpecConfig
import akka.projection.jdbc.JdbcOffsetStoreSpec.PureJdbcSession
import akka.projection.jdbc.internal.Dialect
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.Tag
import org.testcontainers.containers.JdbcDatabaseContainer
import org.testcontainers.containers.MSSQLServerContainer
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.OracleContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy

object JdbcContainerOffsetStoreSpec {

  abstract class ContainerJdbcSpecConfig(dialect: String) extends JdbcSpecConfig {

    val tag: Tag = TestTags.ContainerDb

    override val config: Config =
      baseConfig.withFallback(ConfigFactory.parseString(s"""
        akka.projection.jdbc = {
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
    override def newContainer() = new PostgreSQLContainer
  }

  object MySQLSpecConfig extends ContainerJdbcSpecConfig("mysql-dialect") {
    val name = "MySQL Database"
    override def newContainer() = new MySQLContainer
  }

  object MSSQLServerSpecConfig extends ContainerJdbcSpecConfig("mssql-dialect") {
    val name = "MS SQL Server Database"
    override val tag: Tag = TestTags.FlakyDb
    override def newContainer() = new MSSQLServerContainer
  }

  object OracleSpecConfig extends ContainerJdbcSpecConfig("oracle-dialect") {
    val name = "Oracle Database"
    override def newContainer() = new OracleContainer("oracleinanutshell/oracle-xe-11g")
  }

}

class PostgresJdbcOffsetStoreSpec extends JdbcOffsetStoreSpec(JdbcContainerOffsetStoreSpec.PostgresSpecConfig)
class MySQLJdbcOffsetStoreSpec extends JdbcOffsetStoreSpec(JdbcContainerOffsetStoreSpec.MySQLSpecConfig) {
  override def selectLastStatement: String = Dialect.removeQuotes(super.selectLastStatement)
}
class MSSQLServerJdbcOffsetStoreSpec extends JdbcOffsetStoreSpec(JdbcContainerOffsetStoreSpec.MSSQLServerSpecConfig)
class OracleJdbcOffsetStoreSpec extends JdbcOffsetStoreSpec(JdbcContainerOffsetStoreSpec.OracleSpecConfig)
