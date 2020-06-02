/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra.internal

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import com.typesafe.config.Config

/**
 * INTERNAL API
 */
@InternalApi
private[projection] case class CassandraSettings(config: Config) {
  val keyspace: String = config.getString("offset-store.keyspace")
  val table: String = config.getString("offset-store.table")
  val sessionConfigPath: String = config.getString("session-config-path")
}

/**
 * INTERNAL API
 */
@akka.annotation.InternalApi
private[projection] object CassandraSettings {

  def apply(system: ActorSystem[_]): CassandraSettings =
    CassandraSettings(system.settings.config.getConfig("akka.projection.cassandra"))
}
