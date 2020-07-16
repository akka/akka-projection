package akka.projection.jdbc.docs

import scala.io.Source

import akka.projection.jdbc.internal.DefaultDialect
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
      val dialectStatements = DefaultDialect(None, "AKKA_PROJECTION_OFFSET_STORE").createTableStatements
      val fromDialect =
        s"""
          |#create-table-default
          |${dialectStatements.mkString("\n")}
          |#create-table-default
          |""".stripMargin
      val fromSqlFile = Source.fromResource("create-table-default.sql").mkString
      normalize(fromSqlFile) shouldBe normalize(fromDialect)
    }

    "match the version used in code (MSSQL Server)" in {
      val dialectStatements = MSSQLServerDialect(None, "AKKA_PROJECTION_OFFSET_STORE").createTableStatements
      val fromDialect =
        s"""
          |#create-table-mssql
          |${dialectStatements.mkString("\n")}
          |#create-table-mssql
          |""".stripMargin
      val fromSqlFile = Source.fromResource("create-table-mssql.sql").mkString
      normalize(fromSqlFile) shouldBe normalize(fromDialect)
    }

    "match the version used in code (MySQL)" in {
      val dialectStatements = MySQLDialect(None, "AKKA_PROJECTION_OFFSET_STORE").createTableStatements
      val fromDialect =
        s"""
          |#create-table-mysql
          |${dialectStatements.mkString("\n")}
          |#create-table-mysql
          |""".stripMargin
      val fromSqlFile = Source.fromResource("create-table-mysql.sql").mkString
      normalize(fromSqlFile) shouldBe normalize(fromDialect)
    }

    "match the version used in code (Oracle)" in {
      val dialectStatements = OracleDialect(None, "AKKA_PROJECTION_OFFSET_STORE").createTableStatements
      val fromDialect =
        s"""
          |#create-table-oracle
          |${dialectStatements.mkString("\n")}
          |#create-table-oracle
          |""".stripMargin
      val fromSqlFile = Source.fromResource("create-table-oracle.sql").mkString
      normalize(fromSqlFile) shouldBe normalize(fromDialect)
    }

  }

  private def normalize(in: String): String = {
    in.toLowerCase // ignore case
      .replaceAll("[\\s]++", " ") // ignore multiple blankspaces
      .trim // trim edges
  }
}
