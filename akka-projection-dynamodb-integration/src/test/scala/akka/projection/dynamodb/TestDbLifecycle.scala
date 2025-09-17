/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb

import java.time.Instant

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._
import scala.util.control.NonFatal

import akka.actor.typed.ActorSystem
import akka.persistence.Persistence
import akka.persistence.dynamodb.DynamoDBSettings
import akka.persistence.dynamodb.internal.InstantFactory
import akka.persistence.dynamodb.util.ClientProvider
import akka.persistence.typed.PersistenceId
import akka.projection.ProjectionId
import akka.projection.dynamodb.scaladsl.CreateTables
import akka.serialization.SerializationExtension
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.QueryRequest

trait TestDbLifecycle extends BeforeAndAfterAll { this: Suite =>
  private val log = LoggerFactory.getLogger(getClass)

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

  // to be able to store events with specific timestamps
  def writeEvent(slice: Int, persistenceId: PersistenceId, seqNr: Long, timestamp: Instant, event: String): Unit = {
    import java.util.{ HashMap => JHashMap }

    import akka.persistence.dynamodb.internal.JournalAttributes._

    log.debug(
      "Write test event [{}] [{}] [{}] at time [{}], slice [{}]",
      persistenceId.id,
      seqNr,
      event,
      timestamp,
      slice)

    val stringSerializer = SerializationExtension(typedSystem).serializerFor(classOf[String])

    val attributes = new JHashMap[String, AttributeValue]
    attributes.put(Pid, AttributeValue.fromS(persistenceId.id))
    attributes.put(SeqNr, AttributeValue.fromN(seqNr.toString))
    attributes.put(EntityTypeSlice, AttributeValue.fromS(s"${persistenceId.entityTypeHint}-$slice"))
    val timestampMicros = InstantFactory.toEpochMicros(timestamp)
    attributes.put(Timestamp, AttributeValue.fromN(timestampMicros.toString))
    attributes.put(EventSerId, AttributeValue.fromN(stringSerializer.identifier.toString))
    attributes.put(EventSerManifest, AttributeValue.fromS(""))
    attributes.put(EventPayload, AttributeValue.fromB(SdkBytes.fromByteArray(stringSerializer.toBinary(event))))
    attributes.put(Writer, AttributeValue.fromS(""))

    val req = PutItemRequest
      .builder()
      .tableName(dynamoDBSettings.journalTable)
      .item(attributes)
      .build()
    Await.result(client.putItem(req).asScala, 10.seconds)
  }

}
