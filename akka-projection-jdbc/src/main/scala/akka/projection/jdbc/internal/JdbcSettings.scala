/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.internal

import scala.concurrent.ExecutionContext

import akka.actor.typed.ActorSystem
import akka.actor.typed.DispatcherSelector
import akka.annotation.InternalApi
import com.typesafe.config.Config
import com.typesafe.config.ConfigException
import com.typesafe.config.ConfigValueType

private[projection] trait JdbcSettingsBase {
  val schema: Option[String]
  val table: String
  val managementTable: String
  val verboseLoggingEnabled: Boolean
  val dialect: Dialect
}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] case class JdbcSettings(config: Config, executionContext: ExecutionContext)
    extends JdbcSettingsBase {

  val schema: Option[String] =
    Option(config.getString("offset-store.schema")).filterNot(_.trim.isEmpty)

  val table: String = config.getString("offset-store.table")

  val managementTable: String = config.getString("offset-store.management-table")

  val verboseLoggingEnabled: Boolean = config.getBoolean("debug.verbose-offset-store-logging")

  val dialect: Dialect = {
    val dialectToLoad = config.getString("dialect")
    if (dialectToLoad.trim.isEmpty)
      throw new IllegalArgumentException(
        s"Dialect type not set. Settings 'akka.projection.jdbc.dialect' currently set to [$dialectToLoad]")

    val useLowerCaseSchema = config.getBoolean("offset-store.use-lowercase-schema")
    dialectToLoad match {
      case "h2-dialect"       => H2Dialect(schema, table, managementTable, useLowerCaseSchema)
      case "postgres-dialect" => PostgresDialect(schema, table, managementTable, useLowerCaseSchema)
      case "mysql-dialect"    => MySQLDialect(schema, table, managementTable)
      case "mssql-dialect"    => MSSQLServerDialect(schema, table, managementTable)
      case "oracle-dialect"   => OracleDialect(schema, table, managementTable)
      case unknown =>
        throw new IllegalArgumentException(
          s"Unknown dialect type: [$unknown]. Check settings 'akka.projection.jdbc.dialect'")
    }

  }

}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] object JdbcSettings {

  val configPath = "akka.projection.jdbc"
  val dispatcherPath: String = configPath + ".use-dispatcher"

  private def checkDispatcherConfig(system: ActorSystem[_]) = {

    val dispatcherConfigPath = system.settings.config.getString(dispatcherPath)
    val config = system.settings.config.getConfig(dispatcherConfigPath)
    val pathToPoolSize = "thread-pool-executor.fixed-pool-size"

    def isEmptyString = {
      config.getValue(pathToPoolSize).valueType() == ConfigValueType.STRING &&
      config.getString(pathToPoolSize).trim.isEmpty
    }

    // the reference config has a thread-pool-executor configured
    // with a invalid pool size. We need to check if users configured it correctly
    // it's also possible that user decide to not use a thread-pool-executor
    // in which case, we have nothing else to check
    if (config.getString("executor") == "thread-pool-executor") {

      // empty string can't be parsed to Int, users probably forgot to configure the pool-size
      if (isEmptyString)
        throw new IllegalArgumentException(
          s"Config value for '$dispatcherConfigPath.$pathToPoolSize' is not configured. " +
          "The thread pool size must be integer and be as large as the JDBC connection pool.")

      try {
        // not only explicit Int, but also Int defined as String are valid, eg: 10 and "10"
        config.getInt(pathToPoolSize)
      } catch {
        case _: ConfigException.WrongType =>
          throw new IllegalArgumentException(
            s"Value [${config.getValue(pathToPoolSize).render()}] is not a valid value for settings '$dispatcherConfigPath.$pathToPoolSize'. " +
            s"Current value is [${config.getValue(pathToPoolSize)}]. " +
            "The thread pool size must be integer and be as large as the JDBC connection pool.")
      }
    }
  }

  def apply(system: ActorSystem[_]): JdbcSettings = {
    checkDispatcherConfig(system)
    val dispatcherConfigPath = system.settings.config.getString(dispatcherPath)
    val blockingDbDispatcher = system.dispatchers.lookup(DispatcherSelector.fromConfig(dispatcherConfigPath))
    JdbcSettings(system.settings.config.getConfig(configPath), blockingDbDispatcher)
  }
}
