/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.internal

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi
private[jdbc] trait Dialect {
  def createTableStatement: String
  def alterTableStatement: String

  def insertOrUpdateStatement: String
  def readOffsetQuery: String
  def clearOffsetStatement: String

}

/**
 * INTERNAL API
 * Defines the basic statements. Dialects may use it or define their own.
 */
@InternalApi
private[jdbc] object DialectDefaults {
  def readOffsetQuery(table: String) =
    s"SELECT * FROM $table WHERE PROJECTION_NAME = ?"

  def clearOffsetStatement(table: String) =
    s"DELETE FROM $table WHERE PROJECTION_NAME = ? AND PROJECTION_KEY = ?"

  def insertOrUpdateStatement(table: String) =
    s"""MERGE INTO "$table" ("PROJECTION_NAME","PROJECTION_KEY","OFFSET","MANIFEST","MERGEABLE","LAST_UPDATED")  VALUES (?,?,?,?,?,?)"""

  // NOTE: keep this aligned with insertOrUpdateStatement columns
  object InsertIndices {
    val PROJECTION_NAME = 1
    val PROJECTION_KEY = 2
    val OFFSET = 3
    val MANIFEST = 4
    val MERGEABLE = 5
    val LAST_UPDATED = 6
  }

}

/**
 * INTERNAL API
 */
@InternalApi
private[jdbc] case class H2Dialect(schema: Option[String], tableName: String) extends Dialect {

  def this(tableName: String) = this(None, tableName)

  private val table = schema.map(s => s"$s.$tableName").getOrElse(tableName)

  override val createTableStatement = s"""
     CREATE TABLE IF NOT EXISTS "$table" (
      "PROJECTION_NAME" CHAR(255) NOT NULL,
      "PROJECTION_KEY" CHAR(255) NOT NULL,
      "OFFSET" CHAR(255) NOT NULL,
      "MANIFEST" VARCHAR(4) NOT NULL,
      "MERGEABLE" BOOLEAN NOT NULL,
      "LAST_UPDATED" TIMESTAMP(9) WITH TIME ZONE NOT NULL
     );
     """

  override val alterTableStatement =
    s"""
       ALTER TABLE "$table" 
       ADD CONSTRAINT "PK_PROJECTION_ID" PRIMARY KEY("PROJECTION_NAME","PROJECTION_KEY");
    """

  override val readOffsetQuery: String = DialectDefaults.readOffsetQuery(table)

  override val insertOrUpdateStatement = DialectDefaults.insertOrUpdateStatement(table)

  override val clearOffsetStatement: String = DialectDefaults.clearOffsetStatement(table)
}
