/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.internal

import scala.concurrent.ExecutionContext

import akka.actor.typed.ActorSystem
import akka.actor.typed.DispatcherSelector
import akka.annotation.InternalApi
import com.typesafe.config.Config
import com.typesafe.config.ConfigValueType

/**
 * INTERNAL API
 */
@InternalApi
private[projection] case class JdbcSettings(config: Config, executionContext: ExecutionContext) {

  val schema: Option[String] =
    Option(config.getString("offset-store.schema")).filterNot(_.trim.isEmpty)

  val table: String = config.getString("offset-store.table")

  val dialect = {
    val dialectToLoad = config.getString("dialect")
    if (dialectToLoad == "<dialect>" || dialectToLoad.trim.isEmpty)
      throw new IllegalArgumentException(
        s"Dialect type not set. Settings 'akka.projection.jdbc.dialect' currently set to $dialectToLoad")

    dialectToLoad match {
      case "h2-dialect" => H2Dialect(schema, table)
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
  val dispatcherConfigPath = configPath + ".blocking-jdbc-dispatcher"

  private def checkDispatcherConfig(system: ActorSystem[_]) = {

    val config = system.settings.config.getConfig(dispatcherConfigPath)
    val pathToPoolSize = "thread-pool-executor.fixed-pool-size"

    // the reference config has a thread-pool-executor configured
    // with a invalid pool size. We need to check if users configured it correctly
    // it's also possible that user decide to not use a thread-pool-executor
    // in which case, we have nothing else to check
    if (config.hasPath(pathToPoolSize)) {
      if (config.getValue(pathToPoolSize).valueType() == ConfigValueType.STRING) {
        throw new IllegalArgumentException(
          s"Config value for '$dispatcherConfigPath.$pathToPoolSize' isn't configured. " +
          "The thread pool size must be as large as the JDBC connection pool.")
      }
    }
  }

  def apply(system: ActorSystem[_]): JdbcSettings = {

    checkDispatcherConfig(system)

    val blockingDbDispatcher = system.dispatchers.lookup(DispatcherSelector.fromConfig(dispatcherConfigPath))
    JdbcSettings(system.settings.config.getConfig(configPath), blockingDbDispatcher)
  }
}
