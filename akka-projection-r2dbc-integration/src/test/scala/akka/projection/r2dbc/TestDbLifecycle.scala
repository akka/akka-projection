/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.r2dbc

import java.time.Instant

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.control.NonFatal

import akka.actor.typed.ActorSystem
import akka.persistence.Persistence
import akka.persistence.r2dbc.ConnectionFactoryProvider
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.r2dbc.internal.codec.PayloadCodec
import akka.persistence.r2dbc.internal.codec.QueryAdapter
import akka.persistence.r2dbc.internal.codec.TimestampCodec
import akka.persistence.r2dbc.internal.codec.TimestampCodec.PostgresTimestampCodec
import akka.persistence.typed.PersistenceId
import akka.serialization.SerializationExtension
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite
import org.slf4j.LoggerFactory

trait TestDbLifecycle extends BeforeAndAfterAll { this: Suite =>

  private val log = LoggerFactory.getLogger(getClass)

  def typedSystem: ActorSystem[_]

  def testConfigPath: String = "akka.projection.r2dbc"

  lazy val r2dbcProjectionSettings: R2dbcProjectionSettings =
    R2dbcProjectionSettings(typedSystem.settings.config.getConfig(testConfigPath))
  lazy val r2dbcSettings: R2dbcSettings =
    R2dbcSettings(
      typedSystem.settings.config
        .getConfig(r2dbcProjectionSettings.useConnectionFactory.replace(".connection-factory", "")))
  lazy val r2dbcExecutor: R2dbcExecutor = {
    new R2dbcExecutor(
      ConnectionFactoryProvider(typedSystem).connectionFactoryFor(r2dbcProjectionSettings.useConnectionFactory),
      LoggerFactory.getLogger(getClass),
      r2dbcProjectionSettings.logDbCallsExceeding,
      ConnectionFactoryProvider(typedSystem)
        .connectionPoolSettingsFor(r2dbcProjectionSettings.useConnectionFactory)
        .closeCallsExceeding)(typedSystem.executionContext, typedSystem)
  }

  lazy val persistenceExt: Persistence = Persistence(typedSystem)

  override protected def beforeAll(): Unit = {
    try {
      Await.result(
        r2dbcExecutor.updateOne("beforeAll delete")(
          _.createStatement(s"delete from ${r2dbcSettings.journalTableWithSchema(0)}")),
        10.seconds)
      Await.result(
        r2dbcExecutor.updateOne("beforeAll delete")(_.createStatement(s"delete from ${r2dbcSettings.snapshotsTable}")),
        10.seconds)
      Await.result(
        r2dbcExecutor.updateOne("beforeAll delete")(
          _.createStatement(s"delete from ${r2dbcSettings.durableStateTableWithSchema(0)}")),
        10.seconds)
      if (r2dbcProjectionSettings.isOffsetTableDefined) {
        Await.result(
          r2dbcExecutor.updateOne("beforeAll delete")(
            _.createStatement(s"delete from ${r2dbcProjectionSettings.offsetTableWithSchema}")),
          10.seconds)
      }
      Await.result(
        r2dbcExecutor.updateOne("beforeAll delete")(
          _.createStatement(s"delete from ${r2dbcProjectionSettings.timestampOffsetTableWithSchema}")),
        10.seconds)
      Await.result(
        r2dbcExecutor.updateOne("beforeAll delete")(
          _.createStatement(s"delete from ${r2dbcProjectionSettings.managementTableWithSchema}")),
        10.seconds)
    } catch {
      case NonFatal(ex) =>
        throw new RuntimeException("Failed to clean up tables before test", ex)
    }
    super.beforeAll()
  }

  // to be able to store events with specific timestamps
  def writeEvent(slice: Int, persistenceId: String, seqNr: Long, timestamp: Instant, event: String): Unit = {
    implicit val payloadCodec: PayloadCodec = r2dbcSettings.codecSettings.JournalImplicits.journalPayloadCodec
    implicit val queryAdapter: QueryAdapter = r2dbcSettings.codecSettings.JournalImplicits.queryAdapter
    import PayloadCodec.RichStatement
    import TimestampCodec.TimestampCodecRichStatement
    import akka.persistence.r2dbc.internal.Sql.InterpolationWithAdapter
    implicit val timestampCodec: TimestampCodec = PostgresTimestampCodec
    val stringSerializer = SerializationExtension(typedSystem).serializerFor(classOf[String])

    log.debug("Write test event [{}] [{}] [{}] at time [{}]", persistenceId, seqNr, event, timestamp)
    val insertEventSql = sql"""
      INSERT INTO ${r2dbcSettings.journalTableWithSchema(slice)}
      (slice, entity_type, persistence_id, seq_nr, db_timestamp, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload)
      VALUES (?, ?, ?, ?, ?, '', '', ?, '', ?)"""
    val entityType = PersistenceId.extractEntityType(persistenceId)

    val result = r2dbcExecutor.updateOne("test writeEvent") { connection =>
      connection
        .createStatement(insertEventSql)
        .bind(0, slice)
        .bind(1, entityType)
        .bind(2, persistenceId)
        .bind(3, seqNr)
        .bindTimestamp(4, timestamp)
        .bind(5, stringSerializer.identifier)
        .bindPayload(6, stringSerializer.toBinary(event))
    }
    Await.result(result, 5.seconds)
  }
}
