/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.dynamodb.scaladsl

import java.util.concurrent.CompletionException

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.FutureConverters._
import scala.util.Failure
import scala.util.Success

import akka.Done
import akka.actor.typed.ActorSystem
import akka.persistence.dynamodb.util.OnDemandThroughputSettings
import akka.persistence.dynamodb.util.ProvisionedThroughputSettings
import akka.persistence.dynamodb.util.TableSettings
import akka.projection.dynamodb.DynamoDBProjectionSettings
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import software.amazon.awssdk.services.dynamodb.model.KeyType
import software.amazon.awssdk.services.dynamodb.model.OnDemandThroughput
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType

object CreateTables {
  def createTimestampOffsetStoreTable(
      system: ActorSystem[_],
      settings: DynamoDBProjectionSettings,
      client: DynamoDbAsyncClient,
      deleteIfExists: Boolean,
      tableSettings: TableSettings = TableSettings.Local): Future[Done] = {
    import akka.projection.dynamodb.internal.OffsetStoreDao.OffsetStoreAttributes._
    implicit val ec: ExecutionContext = system.executionContext

    val existingTable =
      client.describeTable(DescribeTableRequest.builder().tableName(settings.timestampOffsetTable).build()).asScala

    def create(): Future[Done] = {
      var requestBuilder = CreateTableRequest.builder
        .tableName(settings.timestampOffsetTable)
        .keySchema(
          KeySchemaElement.builder().attributeName(NameSlice).keyType(KeyType.HASH).build(),
          KeySchemaElement.builder().attributeName(Pid).keyType(KeyType.RANGE).build())
        .attributeDefinitions(
          AttributeDefinition.builder().attributeName(NameSlice).attributeType(ScalarAttributeType.S).build(),
          AttributeDefinition.builder().attributeName(Pid).attributeType(ScalarAttributeType.S).build())

      requestBuilder = tableSettings.throughput match {
        case provisioned: ProvisionedThroughputSettings =>
          requestBuilder.provisionedThroughput(
            ProvisionedThroughput.builder
              .readCapacityUnits(provisioned.readCapacityUnits)
              .writeCapacityUnits(provisioned.writeCapacityUnits)
              .build())
        case onDemand: OnDemandThroughputSettings =>
          requestBuilder.onDemandThroughput(
            OnDemandThroughput.builder
              .maxReadRequestUnits(onDemand.maxReadRequestUnits)
              .maxWriteRequestUnits(onDemand.maxWriteRequestUnits)
              .build())
      }

      client
        .createTable(requestBuilder.build())
        .asScala
        .map(_ => Done)
        .recoverWith {
          case c: CompletionException =>
            Future.failed(c.getCause)
        }(ExecutionContext.parasitic)
    }

    def delete(): Future[Done] = {
      val req = DeleteTableRequest.builder().tableName(settings.timestampOffsetTable).build()
      client
        .deleteTable(req)
        .asScala
        .map(_ => Done)
        .recoverWith {
          case c: CompletionException =>
            Future.failed(c.getCause)
        }(ExecutionContext.parasitic)
    }

    existingTable.transformWith {
      case Success(_) =>
        if (deleteIfExists) delete().flatMap(_ => create())
        else Future.successful(Done)
      case Failure(_: ResourceNotFoundException) => create()
      case Failure(exception: CompletionException) =>
        exception.getCause match {
          case _: ResourceNotFoundException => create()
          case cause                        => Future.failed[Done](cause)
        }
      case Failure(exc) =>
        Future.failed[Done](exc)
    }
  }

}
