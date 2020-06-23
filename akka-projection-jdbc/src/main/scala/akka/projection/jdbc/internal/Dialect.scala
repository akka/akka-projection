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

  def readOffsetQuery: String
  def clearOffsetStatement: String
  def insertStatement(): String
  def updateStatement(): String

}

/**
 * INTERNAL API
 * Defines the basic statements. Dialects may use it or define their own.
 */
@InternalApi
private[jdbc] object DialectDefaults {
  def readOffsetQuery(table: String) =
    s"""SELECT * FROM "$table" WHERE "PROJECTION_NAME" = ?"""

  def clearOffsetStatement(table: String) =
    s"""DELETE FROM "$table" WHERE "PROJECTION_NAME" = ? AND "PROJECTION_KEY" = ?"""

  def insertStatement(table: String): String =
    s"""INSERT INTO "$table" ("PROJECTION_NAME","PROJECTION_KEY","OFFSET","MANIFEST","MERGEABLE","LAST_UPDATED")  VALUES (?,?,?,?,?,?)"""

  def updateStatement(table: String): String =
    s"""UPDATE "$table" 
       | SET 
       |  "OFFSET" = ?,
       |  "MANIFEST" = ?,
       |  "MERGEABLE" = ?,
       |  "LAST_UPDATED" = ?
       | WHERE "PROJECTION_NAME" = ? AND "PROJECTION_KEY" = ?
       |""".stripMargin

  object InsertIndices {
    val PROJECTION_NAME = 1
    val PROJECTION_KEY = 2
    val OFFSET = 3
    val MANIFEST = 4
    val MERGEABLE = 5
    val LAST_UPDATED = 6
  }

  object UpdateIndices {
    // SET
    val OFFSET = 1
    val MANIFEST = 2
    val MERGEABLE = 3
    val LAST_UPDATED = 4
    // WHERE
    val PROJECTION_NAME = 5
    val PROJECTION_KEY = 6
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
      "PROJECTION_NAME" VARCHAR(255) NOT NULL,
      "PROJECTION_KEY" VARCHAR(255) NOT NULL,
      "OFFSET" VARCHAR(255) NOT NULL,
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

  override val clearOffsetStatement: String = DialectDefaults.clearOffsetStatement(table)

  override def insertStatement(): String = DialectDefaults.insertStatement(table)

  override def updateStatement(): String = DialectDefaults.updateStatement(table)
}
