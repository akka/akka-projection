/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.cassandra

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.projection.ProjectionId
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.SourceProvider
import akka.stream.alpakka.cassandra.scaladsl.CassandraSession
import akka.stream.scaladsl.Source
import org.slf4j.LoggerFactory

object WordCountDocExample {

  //#envelope
  type Word = String
  type Count = Int

  final case class WordEnvelope(offset: Long, word: Word)

  //#envelope

  //#repository
  trait WordCountRepository {
    def load(id: String, word: Word): Future[Count]
    def loadAll(id: String): Future[Map[Word, Count]]
    def save(id: String, word: Word, count: Count): Future[Done]
  }
  //#repository

  class CassandraWordCountRepository(session: CassandraSession)(implicit val ec: ExecutionContext)
      extends WordCountRepository {
    val keyspace = "test"
    val table = "wordcount"

    override def load(id: String, word: Word): Future[Count] = {
      session.selectOne(s"SELECT count FROM $keyspace.$table WHERE id = ? and word = ?", id, word).map {
        case Some(row) => row.getInt("count")
        case None      => 0
      }
    }

    override def loadAll(id: String): Future[Map[Word, Count]] = {
      session.selectAll(s"SELECT word, count FROM $keyspace.$table WHERE id = ?", id).map { rows =>
        rows.map(row => row.getString("word") -> row.getInt("count")).toMap
      }
    }

    override def save(id: String, word: Word, count: Count): Future[Done] = {
      session.executeWrite(
        s"INSERT INTO $keyspace.$table (id, word, count) VALUES (?, ?, ?)",
        id,
        word,
        count: java.lang.Integer)
    }

    def createKeyspaceAndTable(): Future[Done] = {
      session
        .executeDDL(
          s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy','replication_factor':1 }")
        .flatMap(_ => session.executeDDL(s"""
        |CREATE TABLE IF NOT EXISTS $keyspace.$table (
        |  id text,
        |  word text,
        |  count int,
        |  PRIMARY KEY (id, word))
        """.stripMargin.trim))
    }
  }

  //#sourceProvider
  class WordSource(implicit ec: ExecutionContext) extends SourceProvider[Long, WordEnvelope] {

    private val src = Source(
      List(WordEnvelope(1L, "abc"), WordEnvelope(2L, "def"), WordEnvelope(3L, "ghi"), WordEnvelope(4L, "abc")))

    override def source(offset: () => Future[Option[Long]]): Future[Source[WordEnvelope, _]] = {
      offset().map {
        case Some(o) => src.dropWhile(_.offset <= o)
        case _       => src
      }
    }

    override def extractOffset(env: WordEnvelope): Long = env.offset
  }
  //#sourceProvider

  object IllustrateVariables {
    //#mutableState
    class WordCountHandler extends Handler[WordEnvelope] {
      private val logger = LoggerFactory.getLogger(getClass)
      private var state: Map[Word, Count] = Map.empty

      override def process(envelope: WordEnvelope): Future[Done] = {
        val word = envelope.word
        val newCount = state.getOrElse(word, 0) + 1
        logger.info("Word count for {} is {}", word, newCount)
        state = state.updated(word, newCount)
        Future.successful(Done)
      }
    }
    //#mutableState
  }

  object IllustrateStatefulHandlerLoadingInitialState {

    //#loadingInitialState
    import akka.projection.scaladsl.StatefulHandler

    class WordCountHandler(projectionId: ProjectionId, repository: WordCountRepository)(implicit ec: ExecutionContext)
        extends StatefulHandler[Map[Word, Count], WordEnvelope] {

      override def initialState(): Future[Map[Word, Count]] = repository.loadAll(projectionId.id)

      override def process(state: Map[Word, Count], envelope: WordEnvelope): Future[Map[Word, Count]] = {
        val word = envelope.word
        val newCount = state.getOrElse(word, 0) + 1
        val newState = for {
          _ <- repository.save(projectionId.id, word, newCount)
        } yield state.updated(word, newCount)

        newState
      }
    }
    //#loadingInitialState
  }

  object IllustrateStatefulHandlerLoadingStateOnDemand {

    //#loadingOnDemand
    import akka.projection.scaladsl.StatefulHandler

    class WordCountHandler(projectionId: ProjectionId, repository: WordCountRepository)(implicit ec: ExecutionContext)
        extends StatefulHandler[Map[Word, Count], WordEnvelope] {

      override def initialState(): Future[Map[Word, Count]] =
        Future.successful(Map.empty)

      override def process(state: Map[Word, Count], envelope: WordEnvelope): Future[Map[Word, Count]] = {
        val word = envelope.word

        val currentCount =
          state.get(word) match {
            case None =>
              repository.load(projectionId.id, word)
            case Some(count) =>
              Future.successful(count)
          }

        val newState = for {
          c <- currentCount
          newCount = c + 1
          _ <- repository.save(projectionId.id, word, newCount)
        } yield state.updated(word, newCount)

        newState
      }

    }
    //#loadingOnDemand
  }

}
