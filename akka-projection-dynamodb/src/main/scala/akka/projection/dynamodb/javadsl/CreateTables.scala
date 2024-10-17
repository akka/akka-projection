/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb.javadsl

import java.util.concurrent.CompletionStage

import scala.jdk.FutureConverters._

import akka.Done
import akka.actor.typed.ActorSystem
import akka.persistence.dynamodb.util.TableSettings
import akka.projection.dynamodb.DynamoDBProjectionSettings
import akka.projection.dynamodb.scaladsl
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

object CreateTables {
  def createTimestampOffsetStoreTable(
      system: ActorSystem[_],
      settings: DynamoDBProjectionSettings,
      client: DynamoDbAsyncClient,
      deleteIfExists: Boolean): CompletionStage[Done] =
    createTimestampOffsetStoreTable(system, settings, client, deleteIfExists, TableSettings.Local)

  def createTimestampOffsetStoreTable(
      system: ActorSystem[_],
      settings: DynamoDBProjectionSettings,
      client: DynamoDbAsyncClient,
      deleteIfExists: Boolean,
      tableSettings: TableSettings): CompletionStage[Done] =
    scaladsl.CreateTables
      .createTimestampOffsetStoreTable(system, settings, client, deleteIfExists, tableSettings)
      .asJava
}
