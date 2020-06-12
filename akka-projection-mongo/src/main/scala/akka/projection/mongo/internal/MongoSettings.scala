/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.mongo.internal

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import com.typesafe.config.Config

/**
 * INTERNAL API
 */
@InternalApi
private[projection] case class MongoSettings(config: Config) {

  val schema: String = config.getString("offset-store.schema")
  val table: String = config.getString("offset-store.table")
}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] object MongoSettings {

  val configPath = "akka.projection.mongo"

  def apply(system: ActorSystem[_]): MongoSettings =
    MongoSettings(system.settings.config.getConfig(configPath))

}
