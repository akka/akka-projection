/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._
import scala.util.control.NonFatal

import akka.actor.typed.ActorSystem
import akka.persistence.Persistence
import akka.persistence.dynamodb.DynamoDBSettings
import akka.persistence.dynamodb.util.ClientProvider
import akka.projection.ProjectionId
import akka.projection.dynamodb.scaladsl.CreateTables
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.QueryRequest

trait TestDbLifecycle extends BeforeAndAfterAll { this: Suite =>

  def typedSystem: ActorSystem[_]

  def testConfigPath: String = "akka.projection.dynamodb"

  lazy val settings: DynamoDBProjectionSettings =
    DynamoDBProjectionSettings(typedSystem.settings.config.getConfig(testConfigPath))

  lazy val dynamoDBSettings: DynamoDBSettings =
    DynamoDBSettings(
      typedSystem.settings.config
        .getConfig(settings.useClient.replace(".client", "")))

  lazy val persistenceExt: Persistence = Persistence(typedSystem)

  lazy val client: DynamoDbAsyncClient = ClientProvider(typedSystem).clientFor(settings.useClient)

  lazy val localDynamoDB: Boolean = ClientProvider(typedSystem).clientSettingsFor(settings.useClient).local.isDefined

  override protected def beforeAll(): Unit = {
    if (localDynamoDB) {
      try {
        Await.result(
          akka.persistence.dynamodb.util.scaladsl.CreateTables
            .createJournalTable(typedSystem, dynamoDBSettings, client, deleteIfExists = true),
          10.seconds)
        Await.result(
          akka.persistence.dynamodb.util.scaladsl.CreateTables
            .createSnapshotsTable(typedSystem, dynamoDBSettings, client, deleteIfExists = true),
          10.seconds)
        Await.result(
          CreateTables.createTimestampOffsetStoreTable(typedSystem, settings, client, deleteIfExists = true),
          10.seconds)
      } catch {
        case NonFatal(ex) => throw new RuntimeException(s"Test db creation failed", ex)
      }
    }

    super.beforeAll()
  }

  // directly get an offset item (timestamp or sequence number) from the timestamp offset table
  def getOffsetItemFor(
      projectionId: ProjectionId,
      slice: Int,
      persistenceId: String = "_"): Option[Map[String, AttributeValue]] = {
    import akka.projection.dynamodb.internal.OffsetStoreDao.OffsetStoreAttributes._

    val attributes =
      Map(
        ":nameSlice" -> AttributeValue.fromS(s"${projectionId.name}-$slice"),
        ":pid" -> AttributeValue.fromS(persistenceId)).asJava

    val request = QueryRequest.builder
      .tableName(settings.timestampOffsetTable)
      .consistentRead(true)
      .keyConditionExpression(s"$NameSlice = :nameSlice AND $Pid = :pid")
      .expressionAttributeValues(attributes)
      .build()

    Await.result(client.query(request).asScala, 10.seconds).items.asScala.headOption.map(_.asScala.toMap)
  }

}
