/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.docs

import scala.io.Source

import akka.projection.jdbc.internal.DefaultDialect
import akka.projection.jdbc.internal.Dialect
import akka.projection.jdbc.internal.MSSQLServerDialect
import akka.projection.jdbc.internal.MySQLDialect
import akka.projection.jdbc.internal.OracleDialect
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

/**
 *
 */
class OffsetSchemaSpec extends AnyWordSpecLike with Matchers {
  "Documentation for JDBC OffsetStore Schema" should {

    "match the version used in code (Default - PostgreSQL/H2)" in {
      val fromDialect = asFileContent(DefaultDialect.apply _, "default")
      val fromSqlFile = Source.fromResource("create-table-default.sql").mkString
      normalize(fromSqlFile) shouldBe normalize(fromDialect)
    }

    "match the version used in code (MSSQL Server)" in {
      val fromDialect = asFileContent(MSSQLServerDialect.apply _, "mssql")
      val fromSqlFile = Source.fromResource("create-table-mssql.sql").mkString
      normalize(fromSqlFile) shouldBe normalize(fromDialect)
    }

    "match the version used in code (MySQL)" in {
      val fromDialect = asFileContent(MySQLDialect.apply _, "mysql")
      val fromSqlFile = Source.fromResource("create-table-mysql.sql").mkString
      normalize(fromSqlFile) shouldBe normalize(fromDialect)
    }

    "match the version used in code (Oracle)" in {
      val fromDialect = asFileContent(OracleDialect.apply _, "oracle")
      val fromSqlFile = Source.fromResource("create-table-oracle.sql").mkString
      normalize(fromSqlFile) shouldBe normalize(fromDialect)
    }

  }

  private def asFileContent(dialect: (Option[String], String) => Dialect, dialectCode: String): String = {
    val dialectStatements = dialect(None, "AKKA_PROJECTION_OFFSET_STORE").createTableStatements
    s"""
         |#create-table-$dialectCode
         |${dialectStatements.mkString("\n")}
         |#create-table-$dialectCode
         |""".stripMargin
  }

  private def normalize(in: String): String = {
    in.toLowerCase // ignore case
      .replaceAll("[\\s]++", " ") // ignore multiple blankspaces
      .trim // trim edges
  }
}
