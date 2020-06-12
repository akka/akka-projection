/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.cassandra

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import akka.Done
import akka.actor.typed.ActorSystem
import akka.projection.ProjectionId
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.SourceProvider
import akka.stream.alpakka.cassandra.scaladsl.CassandraSession
import akka.stream.scaladsl.Source
import akka.util.Timeout
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

  object IllstrateActorLoadingInitialState {
    import akka.actor.typed.ActorRef
    import akka.actor.typed.Behavior

    //#actorHandler
    import akka.projection.scaladsl.ActorHandler

    class WordCountActorHandler(behavior: Behavior[WordCountProcessor.Command])(implicit system: ActorSystem[_])
        extends ActorHandler[WordEnvelope, WordCountProcessor.Command](behavior) {
      import akka.actor.typed.scaladsl.AskPattern._
      import system.executionContext

      private implicit val askTimeout: Timeout = 5.seconds

      override def process(envelope: WordEnvelope, actor: ActorRef[WordCountProcessor.Command]): Future[Done] = {
        actor.ask[Try[Done]](replyTo => WordCountProcessor.Handle(envelope, replyTo)).map {
          case Success(_)   => Done
          case Failure(exc) => throw exc
        }
      }
    }
    //#actorHandler

    //#behaviorLoadingInitialState
    import akka.actor.typed.ActorRef
    import akka.actor.typed.Behavior
    import akka.actor.typed.SupervisorStrategy
    import akka.actor.typed.scaladsl.ActorContext
    import akka.actor.typed.scaladsl.Behaviors

    object WordCountProcessor {
      trait Command
      final case class Handle(envelope: WordEnvelope, replyTo: ActorRef[Try[Done]]) extends Command
      case object Stop extends Command
      private final case class InitialState(state: Map[Word, Count]) extends Command
      private final case class SaveCompleted(word: Word, saveResult: Try[Done]) extends Command

      def apply(projectionId: ProjectionId, repository: WordCountRepository): Behavior[Command] =
        Behaviors
          .supervise[Command] {
            Behaviors.setup { context =>
              new WordCountProcessor(context, projectionId, repository).init()
            }
          }
          .onFailure(SupervisorStrategy.restartWithBackoff(1.second, 10.seconds, 0.1))
    }

    class WordCountProcessor(
        context: ActorContext[WordCountProcessor.Command],
        projectionId: ProjectionId,
        repository: WordCountRepository) {
      import WordCountProcessor._

      // loading initial state from db
      def init(): Behavior[Command] = {
        Behaviors.withStash(10) { buffer =>
          context.pipeToSelf(repository.loadAll(projectionId.id)) {
            case Success(value) => InitialState(value)
            case Failure(exc)   => throw exc
          }

          Behaviors.receiveMessage {
            case InitialState(state) =>
              context.log.debug("Initial state [{}]", state)
              buffer.unstashAll(idle(state))
            case other =>
              context.log.debug("Stashed [{}]", other)
              buffer.stash(other)
              Behaviors.same
          }
        }
      }

      // waiting for next envelope
      private def idle(state: Map[Word, Count]): Behavior[Command] =
        Behaviors.receiveMessagePartial {
          case Handle(envelope, replyTo) =>
            val word = envelope.word
            context.pipeToSelf(repository.save(projectionId.id, word, state.getOrElse(word, 0) + 1)) { saveResult =>
              SaveCompleted(word, saveResult)
            }
            saving(state, replyTo) // will reply from SaveCompleted
          case Stop =>
            Behaviors.stopped
          case _: InitialState =>
            Behaviors.unhandled
        }

      // saving the new count for a word in db
      private def saving(state: Map[Word, Count], replyTo: ActorRef[Try[Done]]): Behavior[Command] =
        Behaviors.receiveMessagePartial {
          case SaveCompleted(word, saveResult) =>
            replyTo ! saveResult
            saveResult match {
              case Success(_)   => idle(state.updated(word, state.getOrElse(word, 0) + 1))
              case Failure(exc) => throw exc // restart, reload state from db
            }
          case Stop =>
            Behaviors.stopped
        }
    }
    //#behaviorLoadingInitialState
  }

  object IllstrateActorLoadingStateOnDemand {
    import akka.actor.typed.ActorRef
    import akka.actor.typed.Behavior
    import akka.actor.typed.SupervisorStrategy
    import akka.actor.typed.scaladsl.ActorContext
    import akka.actor.typed.scaladsl.Behaviors
    import akka.projection.scaladsl.ActorHandler

    class WordCountActorHandler(behavior: Behavior[WordCountProcessor.Command])(implicit system: ActorSystem[_])
        extends ActorHandler[WordEnvelope, WordCountProcessor.Command](behavior) {
      import akka.actor.typed.scaladsl.AskPattern._
      import system.executionContext

      private implicit val askTimeout: Timeout = 5.seconds

      override def process(envelope: WordEnvelope, actor: ActorRef[WordCountProcessor.Command]): Future[Done] = {
        actor.ask[Try[Done]](replyTo => WordCountProcessor.Handle(envelope, replyTo)).map {
          case Success(_)   => Done
          case Failure(exc) => throw exc
        }
      }
    }

    //#behaviorLoadingOnDemand
    object WordCountProcessor {
      trait Command
      final case class Handle(envelope: WordEnvelope, replyTo: ActorRef[Try[Done]]) extends Command
      case object Stop extends Command
      private final case class LoadCompleted(word: Word, loadResult: Try[Count]) extends Command
      private final case class SaveCompleted(word: Word, saveResult: Try[Done]) extends Command

      def apply(projectionId: ProjectionId, repository: WordCountRepository): Behavior[Command] =
        Behaviors
          .supervise[Command] {
            Behaviors.setup[Command] { context =>
              new WordCountProcessor(context, projectionId, repository).idle(Map.empty)
            }
          }
          .onFailure(SupervisorStrategy.restartWithBackoff(1.second, 10.seconds, 0.1))
    }

    class WordCountProcessor(
        context: ActorContext[WordCountProcessor.Command],
        projectionId: ProjectionId,
        repository: WordCountRepository) {
      import WordCountProcessor._

      // waiting for next envelope
      private def idle(state: Map[Word, Count]): Behavior[Command] =
        Behaviors.receiveMessagePartial {
          case Handle(envelope, replyTo) =>
            val word = envelope.word
            state.get(word) match {
              case None =>
                load(word)
                loading(state, replyTo) // will continue from LoadCompleted
              case Some(count) =>
                save(word, count + 1)
                saving(state, replyTo) // will reply from SaveCompleted
            }
          case Stop =>
            Behaviors.stopped
        }

      private def load(word: String): Unit = {
        context.pipeToSelf(repository.load(projectionId.id, word)) { loadResult =>
          LoadCompleted(word, loadResult)
        }
      }

      // loading the count for a word from db
      private def loading(state: Map[Word, Count], replyTo: ActorRef[Try[Done]]): Behavior[Command] =
        Behaviors.receiveMessagePartial {
          case LoadCompleted(word, loadResult) =>
            loadResult match {
              case Success(count) =>
                save(word, count + 1)
                saving(state, replyTo) // will reply from SaveCompleted
              case Failure(exc) =>
                replyTo ! Failure(exc)
                idle(state)
            }
          case Stop =>
            Behaviors.stopped
        }

      private def save(word: String, count: Count): Unit = {
        context.pipeToSelf(repository.save(projectionId.id, word, count)) { saveResult =>
          SaveCompleted(word, saveResult)
        }
      }

      // saving the new count for a word in db
      private def saving(state: Map[Word, Count], replyTo: ActorRef[Try[Done]]): Behavior[Command] =
        Behaviors.receiveMessagePartial {
          case SaveCompleted(word, saveResult) =>
            replyTo ! saveResult
            saveResult match {
              case Success(_) =>
                idle(state.updated(word, state.getOrElse(word, 0) + 1))
              case Failure(_) =>
                // remove the word from the state if the save failed, because it could have been a timeout
                // so that it was actually saved, best to reload
                idle(state - word)
            }
          case Stop =>
            Behaviors.stopped
        }
    }
    //#behaviorLoadingOnDemand
  }

}
