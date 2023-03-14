/*
 * Copyright (C) 2020-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.internal

import scala.collection.immutable

import akka.annotation.InternalApi
import akka.util.Helpers.toRootLowerCase

import java.util.Locale

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

  def schema: Option[String]
  def tableName: String
  def managementTableName: String

  def createTableStatements: immutable.Seq[String]
  def dropTableStatement: String

  def readOffsetQuery: String
  def clearOffsetStatement: String
  def insertStatement(): String
  def updateStatement(): String

  def createManagementTableStatements: immutable.Seq[String]
  def dropManagementTableStatement: String
  def readManagementStateQuery: String
  def insertManagementStatement(): String
  def updateManagementStatement(): String

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
      s"""CREATE INDEX IF NOT EXISTS "AKKA_PROJECTION_NAME_INDEX" on $table ("PROJECTION_NAME");""")

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

  def createManagementTableStatement(table: String): immutable.Seq[String] =
    immutable.Seq(s"""CREATE TABLE IF NOT EXISTS $table (
         |  "PROJECTION_NAME" VARCHAR(255) NOT NULL,
         |  "PROJECTION_KEY" VARCHAR(255) NOT NULL,
         |  "PAUSED" BOOLEAN NOT NULL,
         |  "LAST_UPDATED" BIGINT NOT NULL,
         |  PRIMARY KEY("PROJECTION_NAME", "PROJECTION_KEY")
         |);""".stripMargin)

  def dropManagementTableStatement(table: String): String =
    s"""DROP TABLE IF EXISTS $table;"""

  def readManagementStateQuery(table: String) =
    s"""SELECT * FROM $table WHERE "PROJECTION_NAME" = ? and "PROJECTION_KEY" = ?"""

  def insertManagementStatement(table: String): String =
    s"""INSERT INTO $table (
       |  "PROJECTION_NAME",
       |  "PROJECTION_KEY",
       |  "PAUSED",
       |  "LAST_UPDATED"
       |) VALUES (?,?,?,?)""".stripMargin

  def updateManagementStatement(table: String): String =
    s"""UPDATE $table
       |SET
       | "PAUSED" = ?,
       | "LAST_UPDATED" = ?
       |WHERE "PROJECTION_NAME" = ? AND "PROJECTION_KEY" = ?""".stripMargin

  object InsertManagementIndices {
    val PROJECTION_NAME = 1
    val PROJECTION_KEY = 2
    val PAUSED = 3
    val LAST_UPDATED = 4
  }

  object UpdateManagementIndices {
    // SET
    val PAUSED = 1
    val LAST_UPDATED = 2
    // WHERE
    val PROJECTION_NAME = 3
    val PROJECTION_KEY = 4
  }

}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] case class H2Dialect(
    schema: Option[String],
    tableName: String,
    managementTableName: String,
    lowerCase: Boolean)
    extends Dialect {

  def this(tableName: String, managementTableName: String, lowerCase: Boolean) =
    this(None, tableName, managementTableName, lowerCase)

  def transform(stmt: String): String =
    if (lowerCase) toRootLowerCase(stmt)
    else stmt

  private val table = transform(schema.map(s => s""""$s"."$tableName"""").getOrElse(s""""$tableName""""))

  private val managementTable = transform(
    schema.map(s => s""""$s"."$managementTableName"""").getOrElse(s""""$managementTableName""""))

  override val createTableStatements: immutable.Seq[String] =
    DialectDefaults.createTableStatement(table).map(s => transform(s))

  override val dropTableStatement: String = transform(DialectDefaults.dropTableStatement(table))

  override val readOffsetQuery: String = transform(DialectDefaults.readOffsetQuery(table))

  override val clearOffsetStatement: String = transform(DialectDefaults.clearOffsetStatement(table))

  override def insertStatement(): String = transform(DialectDefaults.insertStatement(table))

  override def updateStatement(): String = transform(DialectDefaults.updateStatement(table))

  override val createManagementTableStatements: immutable.Seq[String] =
    DialectDefaults.createManagementTableStatement(managementTable).map(s => transform(s))

  override val dropManagementTableStatement: String = transform(
    DialectDefaults.dropManagementTableStatement(managementTable))

  override val readManagementStateQuery: String = transform(DialectDefaults.readManagementStateQuery(managementTable))

  override def insertManagementStatement(): String =
    transform(DialectDefaults.insertManagementStatement(managementTable))

  override def updateManagementStatement(): String =
    transform(DialectDefaults.updateManagementStatement(managementTable))

}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] case class PostgresDialect(
    schema: Option[String],
    tableName: String,
    managementTableName: String,
    lowerCase: Boolean)
    extends Dialect {

  def this(tableName: String, managementTableName: String, lowerCase: Boolean) =
    this(None, tableName, managementTableName, lowerCase)

  def transform(stmt: String): String =
    if (lowerCase) toRootLowerCase(Dialect.removeQuotes(stmt))
    else stmt

  private val table = {
    schema
      .map { s =>
        transform(s""""$s"."$tableName"""")
      }
      .getOrElse(transform(s""""$tableName""""))
  }

  private val managementTable = {
    schema
      .map { s =>
        transform(s""""$s"."$managementTableName"""")
      }
      .getOrElse(transform(s""""$managementTableName""""))
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

  override val createManagementTableStatements =
    DialectDefaults.createManagementTableStatement(managementTable).map(s => transform(s))

  override def dropManagementTableStatement: String =
    transform(DialectDefaults.dropManagementTableStatement(managementTable))

  override val readManagementStateQuery: String =
    transform(DialectDefaults.readManagementStateQuery(managementTable))

  override def insertManagementStatement(): String =
    transform(DialectDefaults.insertManagementStatement(managementTable))

  override def updateManagementStatement(): String =
    transform(DialectDefaults.updateManagementStatement(managementTable))

}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] case class MySQLDialect(schema: Option[String], tableName: String, managementTableName: String)
    extends Dialect {

  def this(tableName: String, managementTableName: String) = this(None, tableName, managementTableName)

  private val table = schema.map(s => s"$s.$tableName").getOrElse(tableName)

  private val managementTable = schema.map(s => s"$s.$managementTableName").getOrElse(managementTableName)

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
      s"""CREATE INDEX akka_projection_name_index ON $table (projection_name);""")

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

  override val createManagementTableStatements =
    immutable.Seq(s"""CREATE TABLE IF NOT EXISTS $managementTable (
         |  projection_name VARCHAR(255) NOT NULL,
         |  projection_key VARCHAR(255) NOT NULL,
         |  paused BOOLEAN NOT NULL,
         |  last_updated BIGINT NOT NULL,
         |  PRIMARY KEY(projection_name, projection_key)
         |);""".stripMargin)

  override val dropManagementTableStatement: String =
    Dialect.removeQuotes(DialectDefaults.dropManagementTableStatement(managementTable))

  override val readManagementStateQuery: String =
    Dialect.removeQuotes(DialectDefaults.readManagementStateQuery(managementTable))

  override def insertManagementStatement(): String =
    Dialect.removeQuotes(DialectDefaults.insertManagementStatement(managementTable))

  override def updateManagementStatement(): String =
    Dialect.removeQuotes(DialectDefaults.updateManagementStatement(managementTable))
}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] case class MSSQLServerDialect(
    schema: Option[String],
    tableName: String,
    managementTableName: String)
    extends Dialect {

  def this(tableName: String, managementTableName: String) = this(None, tableName, managementTableName)

  private val table = schema.map(s => s"""$s.$tableName""").getOrElse(s"""$tableName""")

  private val managementTable = schema.map(s => s"""$s.$managementTableName""").getOrElse(s"""$managementTableName""")

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
         |  create index akka_projection_name_index on $table (projection_name)
         |end""".stripMargin)

  override val dropTableStatement: String = DialectDefaults.dropTableStatement(table)

  override val readOffsetQuery: String = DialectDefaults.readOffsetQuery(table)

  override val clearOffsetStatement: String = DialectDefaults.clearOffsetStatement(table)

  override def insertStatement(): String = DialectDefaults.insertStatement(table)

  override def updateStatement(): String = DialectDefaults.updateStatement(table)

  override val createManagementTableStatements =
    immutable.Seq(
      s"""IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'$managementTable') AND type in (N'U'))
         |begin
         |  create table $managementTable (
         |    projection_name VARCHAR(255) NOT NULL,
         |    projection_key VARCHAR(255) NOT NULL,
         |    paused BIT NOT NULL,
         |    last_updated BIGINT NOT NULL
         |  )
         |
         |  alter table $managementTable add constraint pk_projection_management_id primary key(projection_name, projection_key)
         |end""".stripMargin)

  override val dropManagementTableStatement: String = DialectDefaults.dropManagementTableStatement(managementTable)

  override val readManagementStateQuery: String = DialectDefaults.readManagementStateQuery(managementTable)

  override def insertManagementStatement(): String = DialectDefaults.insertManagementStatement(managementTable)

  override def updateManagementStatement(): String = DialectDefaults.updateManagementStatement(managementTable)
}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] case class OracleDialect(_schema: Option[String], _tableName: String, _managementTableName: String)
    extends Dialect {

  def this(tableName: String, managementTableName: String) = this(None, tableName, managementTableName)

  override val schema: Option[String] = _schema.map(s => s.toUpperCase(Locale.ROOT))

  override val tableName: String = _tableName.toUpperCase(Locale.ROOT)

  override val managementTableName: String = _managementTableName.toUpperCase(Locale.ROOT)

  private val table = schema.map(s => s""""$s"."$tableName"""").getOrElse(s""""$tableName"""")

  private val managementTable =
    schema.map(s => s""""$s"."$managementTableName"""").getOrElse(s""""$managementTableName"""")

  override val createTableStatements =
    immutable.Seq(s"""
         |BEGIN
         |
         |  execute immediate 'create table $table ("PROJECTION_NAME" VARCHAR2(255) NOT NULL,"PROJECTION_KEY" VARCHAR2(255) NOT NULL,"CURRENT_OFFSET" VARCHAR2(255) NOT NULL,"MANIFEST" VARCHAR2(4) NOT NULL,"MERGEABLE" CHAR(1) NOT NULL check ("MERGEABLE" in (0, 1)),"LAST_UPDATED" NUMBER(19) NOT NULL) ';
         |  execute immediate 'alter table $table add constraint "PK_PROJECTION_ID" primary key("PROJECTION_NAME","PROJECTION_KEY") ';
         |  execute immediate 'create index "AKKA_PROJECTION_NAME_INDEX" on $table ("PROJECTION_NAME") ';
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

  override val createManagementTableStatements =
    immutable.Seq(s"""
         |BEGIN
         |
         |  execute immediate 'create table $managementTable ("PROJECTION_NAME" VARCHAR2(255) NOT NULL,"PROJECTION_KEY" VARCHAR2(255) NOT NULL,"PAUSED" CHAR(1) NOT NULL check ("PAUSED" in (0, 1)),"LAST_UPDATED" NUMBER(19) NOT NULL) ';
         |  execute immediate 'alter table $managementTable add constraint "PK_PROJECTION_MANAGEMENT_ID" primary key("PROJECTION_NAME","PROJECTION_KEY") ';
         |  EXCEPTION
         |    WHEN OTHERS THEN
         |      IF SQLCODE = -955 THEN
         |        NULL; -- suppresses ORA-00955 exception
         |      ELSE
         |         RAISE;
         |      END IF;
         |END;""".stripMargin)

  override val dropManagementTableStatement: String =
    s"""BEGIN
       |   EXECUTE IMMEDIATE 'DROP TABLE $managementTable';
       |EXCEPTION
       |   WHEN OTHERS THEN
       |      IF SQLCODE != -942 THEN
       |         RAISE;
       |      END IF;
       |END;""".stripMargin

  override val readManagementStateQuery: String = DialectDefaults.readManagementStateQuery(managementTable)

  override def insertManagementStatement(): String = DialectDefaults.insertManagementStatement(managementTable)

  override def updateManagementStatement(): String = DialectDefaults.updateManagementStatement(managementTable)
}
