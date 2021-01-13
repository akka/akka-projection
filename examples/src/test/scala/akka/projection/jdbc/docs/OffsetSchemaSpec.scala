/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.docs

import scala.io.Source

import akka.projection.jdbc.internal.Dialect
import akka.projection.jdbc.internal.H2Dialect
import akka.projection.jdbc.internal.MSSQLServerDialect
import akka.projection.jdbc.internal.MySQLDialect
import akka.projection.jdbc.internal.OracleDialect
import akka.projection.jdbc.internal.PostgresDialect
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class OffsetSchemaSpec extends AnyWordSpecLike with Matchers {
  "Documentation for JDBC OffsetStore Schema" should {

    "match the version used in code (H2)" in {
      val fromDialect = asFileContent((opt, s) => H2Dialect(opt, s, lowerCase = true), "h2")
      val fromSqlFile = Source.fromResource("create-table-h2.sql").mkString
      normalize(fromSqlFile) shouldBe normalize(fromDialect)
    }

    "match the version used in code (PostgreSQL)" in {
      val fromDialect = asFileContent((opt, s) => PostgresDialect(opt, s, lowerCase = true), "postgres")
      val fromSqlFile = Source.fromResource("create-table-postgres.sql").mkString
      normalize(fromSqlFile) shouldBe normalize(fromDialect)
    }

    "match the version used in code (PostgreSQL uppercase)" in {
      val fromDialect = asFileContent((opt, s) => PostgresDialect(opt, s, lowerCase = false), "postgres-uppercase")
      val fromSqlFile = Source.fromResource("create-table-postgres-uppercase.sql").mkString
      normalize(fromSqlFile) shouldBe normalize(fromDialect)
    }

    "match the version used in code (MSSQL Server)" in {
      val fromDialect = asFileContent(MSSQLServerDialect.apply, "mssql")
      val fromSqlFile = Source.fromResource("create-table-mssql.sql").mkString
      normalize(fromSqlFile) shouldBe normalize(fromDialect)
    }

    "match the version used in code (MySQL)" in {
      val fromDialect = asFileContent(MySQLDialect.apply, "mysql")
      val fromSqlFile = Source.fromResource("create-table-mysql.sql").mkString
      normalize(fromSqlFile) shouldBe normalize(fromDialect)
    }

    "match the version used in code (Oracle)" in {
      val fromDialect = asFileContent(OracleDialect.apply, "oracle")
      val fromSqlFile = Source.fromResource("create-table-oracle.sql").mkString
      normalize(fromSqlFile) shouldBe normalize(fromDialect)
    }

  }

  private def asFileContent(dialect: (Option[String], String) => Dialect, dialectCode: String): String = {
    val dialectStatements = dialect(None, "akka_projection_offset_store").createTableStatements
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
