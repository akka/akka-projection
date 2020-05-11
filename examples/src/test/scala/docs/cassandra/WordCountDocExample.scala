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

  object IllstrateStatefulHandlerLoadingInitialState {

    //#loadingInitialState
    class WordCountHandler(projectionId: ProjectionId, repository: WordCountRepository)(implicit ec: ExecutionContext)
        extends Handler[WordEnvelope] {

      private var state: Future[Map[Word, Count]] = repository.loadAll(projectionId.id)

      override def process(envelope: WordEnvelope): Future[Done] = {
        if (state.failed.isCompleted) {
          // reload initial state if initial loadAll failed or previous save failed
          state = repository.loadAll(projectionId.id)
        }

        val word = envelope.word
        val oldState = state
        val newState = for {
          s <- oldState
          newCount = s.getOrElse(word, 0) + 1
          _ <- repository.save(projectionId.id, word, newCount)
        } yield s.updated(word, newCount)

        state = newState

        newState.map(_ => Done)
      }
    }
    //#loadingInitialState
  }

  object IllstrateStatefulHandlerLoadingStateOnDemand {

    //#loadingOnDemand
    class WordCountHandler(projectionId: ProjectionId, repository: WordCountRepository)(implicit ec: ExecutionContext)
        extends Handler[WordEnvelope] {

      private var state: Future[Map[Word, Count]] = Future.successful(Map.empty)

      override def process(envelope: WordEnvelope): Future[Done] = {
        val word = envelope.word
        val oldState = state

        val currentCount =
          oldState.flatMap { s =>
            s.get(word) match {
              case None =>
                repository.load(projectionId.id, word)
              case Some(count) =>
                Future.successful(count)
            }
          }

        val newState = for {
          s <- oldState
          c <- currentCount
          newCount = c + 1
          _ <- repository.save(projectionId.id, word, newCount)
        } yield s.updated(word, newCount)

        // remove the word from the state if the save failed, because it could have been a timeout
        // so that it was actually saved, best to reload
        state = newState.recoverWith(_ => newState.map(_ - word))

        newState.map(_ => Done)
      }
    }
    //#loadingOnDemand
  }

}
