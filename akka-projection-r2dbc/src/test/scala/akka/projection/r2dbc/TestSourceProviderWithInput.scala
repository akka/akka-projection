/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc

import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.jdk.CollectionConverters._

import akka.NotUsed
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.Persistence
import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.EventTimestampQuery
import akka.persistence.query.typed.scaladsl.LoadEventQuery
import akka.projection.BySlicesSourceProvider
import akka.projection.scaladsl.SourceProvider
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source

class TestSourceProviderWithInput()(implicit val system: ActorSystem[_])
    extends SourceProvider[TimestampOffset, EventEnvelope[String]]
    with BySlicesSourceProvider
    with EventTimestampQuery
    with LoadEventQuery {

  private implicit val ec: ExecutionContext = system.executionContext
  private val persistenceExt = Persistence(system)

  private val _input = new AtomicReference[Promise[ActorRef[EventEnvelope[String]]]](Promise())

  def input: Future[ActorRef[EventEnvelope[String]]] = _input.get().future

  private val envelopes = new ConcurrentLinkedQueue[EventEnvelope[String]]

  override def source(offset: () => Future[Option[TimestampOffset]]): Future[Source[EventEnvelope[String], NotUsed]] = {
    val oldPromise = _input.get()
    _input.set(Promise())
    offset().map { _ =>
      Source
        .actorRef[EventEnvelope[String]](
          PartialFunction.empty,
          PartialFunction.empty,
          bufferSize = 1024,
          OverflowStrategy.fail)
        .map { env =>
          envelopes.offer(env)
          env
        }
        .mapMaterializedValue { ref =>
          val typedRef = ref.toTyped[EventEnvelope[String]]
          oldPromise.trySuccess(typedRef)
          _input.get().trySuccess(typedRef)
          NotUsed
        }
    }
  }

  override def extractOffset(envelope: EventEnvelope[String]): TimestampOffset =
    envelope.offset.asInstanceOf[TimestampOffset]

  override def extractCreationTime(envelope: EventEnvelope[String]): Long =
    envelope.timestamp

  override def minSlice: Int = 0

  override def maxSlice: Int = persistenceExt.numberOfSlices - 1

  override def timestampOf(persistenceId: String, sequenceNr: Long): Future[Option[Instant]] = {
    Future.successful(envelopes.iterator().asScala.collectFirst {
      case env
          if env.persistenceId == persistenceId && env.sequenceNr == sequenceNr && env.offset
            .isInstanceOf[TimestampOffset] =>
        env.offset.asInstanceOf[TimestampOffset].timestamp
    })
  }

  override def loadEnvelope[Event](persistenceId: String, sequenceNr: Long): Future[EventEnvelope[Event]] = {
    envelopes.iterator().asScala.collectFirst {
      case env if env.persistenceId == persistenceId && env.sequenceNr == sequenceNr =>
        env.asInstanceOf[EventEnvelope[Event]]
    } match {
      case Some(env) => Future.successful(env)
      case None =>
        Future.failed(
          new NoSuchElementException(
            s"Event with persistenceId [$persistenceId] and sequenceNr [$sequenceNr] not found."))
    }
  }
}
