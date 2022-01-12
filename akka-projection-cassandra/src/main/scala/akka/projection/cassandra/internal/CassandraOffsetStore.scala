/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra.internal

import java.time.Clock
import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.projection.MergeableOffset
import akka.projection.ProjectionId
import akka.projection.internal.ManagementState
import akka.projection.internal.OffsetSerialization
import akka.projection.internal.OffsetSerialization.SingleOffset
import akka.stream.alpakka.cassandra.scaladsl.CassandraSession
import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.cql.Statement

/**
 * INTERNAL API
 */
@InternalApi private[projection] class CassandraOffsetStore(system: ActorSystem[_], clock: Clock) {
  private val offsetSerialization = new OffsetSerialization(system)
  import offsetSerialization.fromStorageRepresentation
  import offsetSerialization.toStorageRepresentation

  private implicit val executionContext: ExecutionContext = system.executionContext
  private val cassandraSettings = CassandraSettings(system)
  private val session: CassandraSession =
    CassandraSessionRegistry(system).sessionFor(cassandraSettings.sessionConfigPath)

  val keyspace: String = cassandraSettings.keyspace
  val table: String = cassandraSettings.table
  val managementTable: String = cassandraSettings.managementTable
  private val cassandraPartitions = 5

  def this(system: ActorSystem[_]) =
    this(system, Clock.systemUTC())

  private def selectOne[T <: Statement[T]](stmt: Statement[T]): Future[Option[Row]] = {
    session.selectOne(stmt.setExecutionProfileName(cassandraSettings.profile))
  }

  private def execute[T <: Statement[T]](stmt: Statement[T]): Future[Done] = {
    session.executeWrite(stmt.setExecutionProfileName(cassandraSettings.profile))
  }

  def readOffset[Offset](projectionId: ProjectionId): Future[Option[Offset]] = {
    val partition = idToPartition(projectionId)
    session
      .prepare(
        s"SELECT projection_key, offset, manifest FROM $keyspace.$table WHERE projection_name = ? AND partition = ? AND projection_key = ?")
      .map(_.bind(projectionId.name, partition, projectionId.key))
      .flatMap(selectOne)
      .map { maybeRow =>
        maybeRow.map(row => fromStorageRepresentation[Offset](row.getString("offset"), row.getString("manifest")))
      }
  }

  def saveOffset[Offset](projectionId: ProjectionId, offset: Offset): Future[Done] = {
    // a partition is calculated to ensure some distribution of projection rows across cassandra nodes, but at the
    // same time let us query all rows for a single projection_name easily
    val partition = idToPartition(projectionId)
    offset match {
      case _: MergeableOffset[_] =>
        throw new IllegalArgumentException("The CassandraOffsetStore does not currently support MergeableOffset")
      case _ =>
        val SingleOffset(_, manifest, offsetStr, _) =
          toStorageRepresentation(projectionId, offset).asInstanceOf[SingleOffset]
        session
          .prepare(
            s"INSERT INTO $keyspace.$table (projection_name, partition, projection_key, offset, manifest, last_updated) VALUES (?, ?, ?, ?, ?, ?)")
          .map(_.bind(projectionId.name, partition, projectionId.key, offsetStr, manifest, Instant.now(clock)))
          .flatMap(execute)
    }
  }

  def clearOffset(projectionId: ProjectionId): Future[Done] = {
    session
      .prepare(s"DELETE FROM $keyspace.$table WHERE projection_name = ? AND partition = ? AND projection_key = ?")
      .map(_.bind(projectionId.name, idToPartition(projectionId), projectionId.key))
      .flatMap(execute)
  }

  def readManagementState(projectionId: ProjectionId): Future[Option[ManagementState]] = {
    val partition = idToPartition(projectionId)
    session
      .prepare(
        s"SELECT paused FROM $keyspace.$managementTable WHERE projection_name = ? AND partition = ? AND projection_key = ?")
      .map(_.bind(projectionId.name, partition, projectionId.key))
      .flatMap(selectOne)
      .map { maybeRow =>
        maybeRow.map(row => ManagementState(paused = row.getBoolean("paused")))
      }
  }

  def savePaused(projectionId: ProjectionId, paused: Boolean): Future[Done] = {
    // Using same partitioning as for offsets.
    // A partition is calculated to ensure some distribution of projection rows across cassandra nodes, but at the
    // same time let us query all rows for a single projection_name easily.
    val partition = idToPartition(projectionId)
    session
      .prepare(
        s"INSERT INTO $keyspace.$managementTable (projection_name, partition, projection_key, paused, last_updated) VALUES (?, ?, ?, ?, ?)")
      .map(_.bind(projectionId.name, partition, projectionId.key, paused: java.lang.Boolean, Instant.now(clock)))
      .flatMap(execute)
  }

  def createKeyspaceAndTable(): Future[Done] = {
    for {
      _ <- session.executeDDL(
        s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy','replication_factor':1 }")
      _ <- session.executeDDL(s"""
        |CREATE TABLE IF NOT EXISTS $keyspace.$table (
        |  projection_name text,
        |  partition int,
        |  projection_key text,
        |  offset text,
        |  manifest text,
        |  last_updated timestamp,
        |  PRIMARY KEY ((projection_name, partition), projection_key))
        """.stripMargin.trim)
      _ <- session.executeDDL(s"""
        |CREATE TABLE IF NOT EXISTS $keyspace.$managementTable (
        |  projection_name text,
        |  partition int,
        |  projection_key text,
        |  paused boolean,
        |  last_updated timestamp,
        |  PRIMARY KEY ((projection_name, partition), projection_key))
        """.stripMargin.trim)
    } yield Done
  }

  private[cassandra] def idToPartition[Offset](projectionId: ProjectionId): Integer = {
    Integer.valueOf(Math.abs(projectionId.key.hashCode() % cassandraPartitions))
  }

}
