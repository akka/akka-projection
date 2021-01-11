/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.internal

import scala.collection.immutable

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi
private[projection] object Dialect {

  def removeQuotes(stmt: String): String = stmt.replace("\"", "")
}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] trait Dialect {

  def createTableStatements: immutable.Seq[String]
  def dropTableStatement: String

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
private[projection] object DialectDefaults {

  def createTableStatement(table: String): immutable.Seq[String] =
    immutable.Seq(
      s"""CREATE TABLE IF NOT EXISTS $table (
         |  "PROJECTION_NAME" VARCHAR(255) NOT NULL,
         |  "PROJECTION_KEY" VARCHAR(255) NOT NULL,
         |  "CURRENT_OFFSET" VARCHAR(255) NOT NULL,
         |  "MANIFEST" VARCHAR(4) NOT NULL,
         |  "MERGEABLE" BOOLEAN NOT NULL,
         |  "LAST_UPDATED" BIGINT NOT NULL,
         |  PRIMARY KEY("PROJECTION_NAME", "PROJECTION_KEY")
         |);""".stripMargin,
      // create index
      s"""CREATE INDEX IF NOT EXISTS "PROJECTION_NAME_INDEX" on $table ("PROJECTION_NAME");""")

  def dropTableStatement(table: String): String =
    s"""DROP TABLE IF EXISTS $table;"""

  def readOffsetQuery(table: String) =
    s"""SELECT * FROM $table WHERE "PROJECTION_NAME" = ?"""

  def clearOffsetStatement(table: String) =
    s"""DELETE FROM $table WHERE "PROJECTION_NAME" = ? AND "PROJECTION_KEY" = ?"""

  def insertStatement(table: String): String =
    s"""INSERT INTO $table (
       |  "PROJECTION_NAME",
       |  "PROJECTION_KEY",
       |  "CURRENT_OFFSET",
       |  "MANIFEST",
       |  "MERGEABLE",
       |  "LAST_UPDATED"
       |) VALUES (?,?,?,?,?,?)""".stripMargin

  def updateStatement(table: String): String =
    s"""UPDATE $table
       |SET
       | "CURRENT_OFFSET" = ?,
       | "MANIFEST" = ?,
       | "MERGEABLE" = ?,
       | "LAST_UPDATED" = ?
       |WHERE "PROJECTION_NAME" = ? AND "PROJECTION_KEY" = ?""".stripMargin

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
private[projection] case class DefaultDialect(schema: Option[String], tableName: String) extends Dialect {

  def this(tableName: String) = this(None, tableName)

  private val table = schema.map(s => s""""$s"."$tableName"""").getOrElse(s""""$tableName"""")

  override val createTableStatements: immutable.Seq[String] = DialectDefaults.createTableStatement(table)

  override val dropTableStatement: String = DialectDefaults.dropTableStatement(table)

  override val readOffsetQuery: String = DialectDefaults.readOffsetQuery(table)

  override val clearOffsetStatement: String = DialectDefaults.clearOffsetStatement(table)

  override def insertStatement(): String = DialectDefaults.insertStatement(table)

  override def updateStatement(): String = DialectDefaults.updateStatement(table)

}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] case class PostgresDialect(schema: Option[String], tableName: String, legacy: Boolean)
    extends Dialect {

  def this(tableName: String, unquoted: Boolean) = this(None, tableName, unquoted)

  def transform(stmt: String): String =
    if (legacy) stmt
    else Dialect.removeQuotes(stmt).toLowerCase

  private val table = {
    schema
      .map { s =>
        transform(s""""$s"."$tableName"""")
      }
      .getOrElse(transform(s""""$tableName""""))
  }

  override val createTableStatements =
    DialectDefaults.createTableStatement(table).map(s => transform(s))

  override def dropTableStatement: String =
    transform(DialectDefaults.dropTableStatement(table))

  override val readOffsetQuery: String =
    transform(DialectDefaults.readOffsetQuery(table))

  override val clearOffsetStatement: String =
    transform(DialectDefaults.clearOffsetStatement(table))

  override def insertStatement(): String =
    transform(DialectDefaults.insertStatement(table))

  override def updateStatement(): String =
    transform(DialectDefaults.updateStatement(table))

}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] case class MySQLDialect(schema: Option[String], tableName: String) extends Dialect {

  def this(tableName: String) = this(None, tableName)

  private val table = schema.map(s => s"$s.$tableName").getOrElse(tableName)

  override val createTableStatements =
    immutable.Seq(
      s"""CREATE TABLE IF NOT EXISTS $table (
         |  projection_name VARCHAR(255) NOT NULL,
         |  projection_key VARCHAR(255) NOT NULL,
         |  current_offset VARCHAR(255) NOT NULL,
         |  manifest VARCHAR(4) NOT NULL,
         |  mergeable BOOLEAN NOT NULL,
         |  last_updated BIGINT NOT NULL,
         |  PRIMARY KEY(projection_name, projection_key)
         |);""".stripMargin,
      // create index
      s"""CREATE INDEX projection_name_index ON $table (projection_name);""")

  override val dropTableStatement: String =
    Dialect.removeQuotes(DialectDefaults.dropTableStatement(table))

  override val readOffsetQuery: String =
    Dialect.removeQuotes(DialectDefaults.readOffsetQuery(table))

  override val clearOffsetStatement: String =
    Dialect.removeQuotes(DialectDefaults.clearOffsetStatement(table))

  override def insertStatement(): String =
    Dialect.removeQuotes(DialectDefaults.insertStatement(table))

  override def updateStatement(): String =
    Dialect.removeQuotes(DialectDefaults.updateStatement(table))
}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] case class MSSQLServerDialect(schema: Option[String], tableName: String) extends Dialect {

  def this(tableName: String) = this(None, tableName)

  private val table = schema.map(s => s"""$s.$tableName""").getOrElse(s"""$tableName""")

  override val createTableStatements =
    immutable.Seq(
      s"""IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'$table') AND type in (N'U'))
         |begin
         |  create table $table (
         |    projection_name VARCHAR(255) NOT NULL,
         |    projection_key VARCHAR(255) NOT NULL,
         |    current_offset VARCHAR(255) NOT NULL,
         |    manifest VARCHAR(4) NOT NULL,
         |    mergeable BIT NOT NULL,
         |    last_updated BIGINT NOT NULL
         |  )
         |
         |  alter table $table add constraint pk_projection_id primary key(projection_name, projection_key)
         |
         |  create index projection_name_index on $table (projection_name)
         |end""".stripMargin)

  override val dropTableStatement: String = DialectDefaults.dropTableStatement(table)

  override val readOffsetQuery: String = DialectDefaults.readOffsetQuery(table)

  override val clearOffsetStatement: String = DialectDefaults.clearOffsetStatement(table)

  override def insertStatement(): String = DialectDefaults.insertStatement(table)

  override def updateStatement(): String = DialectDefaults.updateStatement(table)
}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] case class OracleDialect(schema: Option[String], tableName: String) extends Dialect {

  def this(tableName: String) = this(None, tableName)

  private val table = schema.map(s => s""""$s"."$tableName"""").getOrElse(s""""$tableName"""")

  override val createTableStatements =
    immutable.Seq(s"""
         |BEGIN
         |
         |  execute immediate 'create table $table ("PROJECTION_NAME" VARCHAR2(255) NOT NULL,"PROJECTION_KEY" VARCHAR2(255) NOT NULL,"CURRENT_OFFSET" VARCHAR2(255) NOT NULL,"MANIFEST" VARCHAR2(4) NOT NULL,"MERGEABLE" CHAR(1) NOT NULL check ("MERGEABLE" in (0, 1)),"LAST_UPDATED" NUMBER(19) NOT NULL) ';
         |  execute immediate 'alter table $table add constraint "PK_PROJECTION_ID" primary key("PROJECTION_NAME","PROJECTION_KEY") ';
         |  execute immediate 'create index "PROJECTION_NAME_INDEX" on $table ("PROJECTION_NAME") ';
         |  EXCEPTION
         |    WHEN OTHERS THEN
         |      IF SQLCODE = -955 THEN
         |        NULL; -- suppresses ORA-00955 exception
         |      ELSE
         |         RAISE;
         |      END IF;
         |END;""".stripMargin)

  override val dropTableStatement: String =
    s"""BEGIN
       |   EXECUTE IMMEDIATE 'DROP TABLE $table';
       |EXCEPTION
       |   WHEN OTHERS THEN
       |      IF SQLCODE != -942 THEN
       |         RAISE;
       |      END IF;
       |END;""".stripMargin

  override val readOffsetQuery: String = DialectDefaults.readOffsetQuery(table)

  override val clearOffsetStatement: String = DialectDefaults.clearOffsetStatement(table)

  override def insertStatement(): String = DialectDefaults.insertStatement(table)

  override def updateStatement(): String = DialectDefaults.updateStatement(table)
}
