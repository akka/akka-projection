/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.internal

import scala.collection.immutable

import akka.annotation.InternalApi
import akka.projection.jdbc.internal.Dialect.removeQuotes

/**
 * INTERNAL API
 */
@InternalApi
private[jdbc] trait Dialect {
  def createTableStatements: immutable.Seq[String]

  def readOffsetQuery: String
  def clearOffsetStatement: String
  def insertStatement(): String
  def updateStatement(): String

}

/**
 * INTERNAL API
 */
@InternalApi
private[jdbc] object Dialect {
  def removeQuotes(stmt: String): String =
    stmt.replace("\"", "")
}

/**
 * INTERNAL API
 * Defines the basic statements. Dialects may use it or define their own.
 */
@InternalApi
private[jdbc] object DialectDefaults {

  def createTableStatement(table: String): immutable.Seq[String] =
    immutable.Seq(
      s"""
     CREATE TABLE IF NOT EXISTS "$table" (
      "PROJECTION_NAME" VARCHAR(255) NOT NULL,
      "PROJECTION_KEY" VARCHAR(255) NOT NULL,
      "OFFSET" VARCHAR(255) NOT NULL,
      "MANIFEST" VARCHAR(4) NOT NULL,
      "MERGEABLE" BOOLEAN NOT NULL,
      "LAST_UPDATED" TIMESTAMP(9) WITH TIME ZONE NOT NULL
     );""",
      // create index
      s"""CREATE INDEX "PROJECTION_NAME_INDEX" on "$table" ("PROJECTION_NAME");""",
      // add primary key
      s"""ALTER TABLE "$table" 
       ADD CONSTRAINT "PK_PROJECTION_ID" PRIMARY KEY("PROJECTION_NAME","PROJECTION_KEY");
    """)

  def readOffsetQuery(table: String) =
    s"""SELECT * FROM "$table" WHERE "PROJECTION_NAME" = ?"""

  def clearOffsetStatement(table: String) =
    s"""DELETE FROM "$table" WHERE "PROJECTION_NAME" = ? AND "PROJECTION_KEY" = ?"""

  def insertStatement(table: String): String =
    s"""INSERT INTO "$table" (
      "PROJECTION_NAME",
      "PROJECTION_KEY",
      "OFFSET",
      "MANIFEST",
      "MERGEABLE",
      "LAST_UPDATED"
    )  VALUES (?,?,?,?,?,?)"""

  def updateStatement(table: String): String =
    s"""UPDATE "$table" 
        SET
         "OFFSET" = ?,
         "MANIFEST" = ?,
         "MERGEABLE" = ?,
         "LAST_UPDATED" = ?
        WHERE "PROJECTION_NAME" = ? AND "PROJECTION_KEY" = ?
        """

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
private[jdbc] case class DefaultDialect(schema: Option[String], tableName: String) extends Dialect {

  def this(tableName: String) = this(None, tableName)

  private val table = schema.map(s => s"$s.$tableName").getOrElse(tableName)

  override val createTableStatements: immutable.Seq[String] = DialectDefaults.createTableStatement(table)

  override val readOffsetQuery: String = DialectDefaults.readOffsetQuery(table)

  override val clearOffsetStatement: String = DialectDefaults.clearOffsetStatement(table)

  override def insertStatement(): String = DialectDefaults.insertStatement(table)

  override def updateStatement(): String = DialectDefaults.updateStatement(table)
}

/**
 * INTERNAL API
 */
@InternalApi
private[jdbc] case class MySQLDialect(schema: Option[String], tableName: String) extends Dialect {

  def this(tableName: String) = this(None, tableName)

  private val table = schema.map(s => s"$s.$tableName").getOrElse(tableName)
  override val createTableStatements =
    immutable.Seq(
      s"""
     CREATE TABLE IF NOT EXISTS $table (
      PROJECTION_NAME VARCHAR(255) NOT NULL,
      PROJECTION_KEY VARCHAR(255) NOT NULL,
      OFFSET VARCHAR(255) NOT NULL,
      MANIFEST VARCHAR(4) NOT NULL,
      MERGEABLE BOOLEAN NOT NULL,
      LAST_UPDATED TEXT NOT NULL
     );
    """,
      // create index
      s"""CREATE INDEX PROJECTION_NAME_INDEX ON $table (PROJECTION_NAME);""",
      // add primary key
      s"""ALTER TABLE $table
       ADD CONSTRAINT PK_PROJECTION_ID PRIMARY KEY(PROJECTION_NAME,PROJECTION_KEY);
       """)

  override val readOffsetQuery: String =
    removeQuotes(DialectDefaults.readOffsetQuery(table))

  override val clearOffsetStatement: String =
    removeQuotes(DialectDefaults.clearOffsetStatement(table))

  override def insertStatement(): String =
    removeQuotes(DialectDefaults.insertStatement(table))

  override def updateStatement(): String =
    removeQuotes(DialectDefaults.updateStatement(table))
}

/**
 * INTERNAL API
 */
@InternalApi
private[jdbc] case class MSSQLServerDialect(schema: Option[String], tableName: String) extends Dialect {

  def this(tableName: String) = this(None, tableName)

  private val table = schema.map(s => s"$s.$tableName").getOrElse(tableName)
  override val createTableStatements =
    immutable.Seq(s"""
      IF  NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'$table') AND type in (N'U'))
      begin
      create table "$table" (
        "PROJECTION_NAME" VARCHAR(255) NOT NULL,
        "PROJECTION_KEY" VARCHAR(255) NOT NULL,
        "OFFSET" VARCHAR(255) NOT NULL,
        "MANIFEST" VARCHAR(4) NOT NULL,
        "MERGEABLE" BIT NOT NULL,
        "LAST_UPDATED" DATETIME2 NOT NULL
        )
      
      alter table "$table" add constraint "PK_PROJECTION_ID" primary key("PROJECTION_NAME","PROJECTION_KEY")
      
      create index "PROJECTION_NAME_INDEX" on "$table" ("PROJECTION_NAME")
      end""")

  override val readOffsetQuery: String = DialectDefaults.readOffsetQuery(table)

  override val clearOffsetStatement: String = DialectDefaults.clearOffsetStatement(table)

  override def insertStatement(): String = DialectDefaults.insertStatement(table)

  override def updateStatement(): String = DialectDefaults.updateStatement(table)
}

/**
 * INTERNAL API
 */
@InternalApi
private[jdbc] case class OracleServerDialect(schema: Option[String], tableName: String) extends Dialect {

  def this(tableName: String) = this(None, tableName)

  private val table = schema.map(s => s"$s.$tableName").getOrElse(tableName)
  override val createTableStatements =
    immutable.Seq(s"""
BEGIN

execute immediate 'create table "$table" ("PROJECTION_NAME" VARCHAR2(255) NOT NULL,"PROJECTION_KEY" VARCHAR2(255) NOT NULL,"OFFSET" VARCHAR2(255) NOT NULL,"MANIFEST" VARCHAR2(4) NOT NULL,"MERGEABLE" CHAR(1) NOT NULL check ("MERGEABLE" in (0, 1)),"LAST_UPDATED" TIMESTAMP(9) WITH TIME ZONE NOT NULL) ';
execute immediate 'alter table "$table" add constraint "PK_PROJECTION_ID" primary key("PROJECTION_NAME","PROJECTION_KEY") ';
execute immediate 'create index "PROJECTION_NAME_INDEX" on "$table" ("PROJECTION_NAME") ';
EXCEPTION
    WHEN OTHERS THEN
      IF SQLCODE = -955 THEN
        NULL; -- suppresses ORA-00955 exception
      ELSE
         RAISE;
      END IF;
END; 
     """)

  override val readOffsetQuery: String = DialectDefaults.readOffsetQuery(table)

  override val clearOffsetStatement: String = DialectDefaults.clearOffsetStatement(table)

  override def insertStatement(): String = DialectDefaults.insertStatement(table)

  override def updateStatement(): String = DialectDefaults.updateStatement(table)
}

/*


 */
