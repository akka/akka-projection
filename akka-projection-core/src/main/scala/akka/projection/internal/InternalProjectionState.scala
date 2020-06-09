/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import akka.Done
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.event.LoggingAdapter
import akka.projection.HandlerRecoveryStrategy
import akka.projection.OffsetVerification.VerificationFailure
import akka.projection.OffsetVerification.VerificationSuccess
import akka.projection.ProjectionId
import akka.projection.RunningProjection
import akka.projection.StatusObserver
import akka.projection.scaladsl.SourceProvider
import akka.stream.KillSwitches
import akka.stream.SharedKillSwitch
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source

abstract class InternalProjectionState[Offset, Envelope](
    projectionId: ProjectionId,
    sourceProvider: SourceProvider[Offset, Envelope],
    offsetStrategy: OffsetStrategy,
    handlerStrategy: HandlerStrategy[Envelope],
    statusObserver: StatusObserver[Envelope],
    settings: ProjectionSettings) {

  def logger: LoggingAdapter
  implicit def system: ActorSystem[_]
  implicit def executionContext: ExecutionContext

  def readOffsets(): Future[Option[Offset]]
  def saveOffset(projectionId: ProjectionId, offset: Offset): Future[Done]
  val killSwitch: SharedKillSwitch = KillSwitches.shared(projectionId.id)
  val abort: Promise[Done] = Promise()

  private def reportProgress[T](after: Future[T], env: Envelope): Future[T] = {
    after.map { done =>
      try {
        statusObserver.progress(projectionId, env)
      } catch {
        case NonFatal(_) => // ignore
      }
      done
    }
  }

  private def atLeastOnceProcessing(
      source: Source[(Offset, Envelope), NotUsed],
      afterEnvelopes: Int,
      orAfterDuration: FiniteDuration,
      recoveryStrategy: HandlerRecoveryStrategy) = {

    val atLeastOnceHandlerFlow: Flow[(Offset, Envelope), (Offset, Envelope), NotUsed] =
      handlerStrategy match {
        case SingleHandlerStrategy(handler) =>
          val handlerRecovery =
            HandlerRecoveryImpl[Offset, Envelope](projectionId, recoveryStrategy, logger, statusObserver)

          Flow[(Offset, Envelope)].mapAsync(parallelism = 1) {
            case elem @ (offset, envelope) =>
              handlerRecovery
                .applyRecovery(envelope, offset, offset, abort.future, () => handler.process(envelope))
                .map(_ => elem)
          }

        case grouped: GroupedHandlerStrategy[Envelope] =>
          val groupAfterEnvelopes = grouped.afterEnvelopes.getOrElse(settings.groupAfterEnvelopes)
          val groupAfterDuration = grouped.orAfterDuration.getOrElse(settings.groupAfterDuration)
          val handlerRecovery =
            HandlerRecoveryImpl[Offset, Envelope](projectionId, recoveryStrategy, logger, statusObserver)

          Flow[(Offset, Envelope)]
            .groupedWithin(groupAfterEnvelopes, groupAfterDuration)
            .filterNot(_.isEmpty)
            .mapAsync(parallelism = 1) { group =>
              val (firstOffset, firstEnvelope) = group.head
              val (lastOffset, lastEnvelope) = group.last
              val envelopes = group.map { case (_, env) => env }
              handlerRecovery
                .applyRecovery(
                  firstEnvelope,
                  firstOffset,
                  lastOffset,
                  abort.future,
                  () => grouped.handler.process(envelopes))
                .map(_ => lastOffset -> lastEnvelope)
            }

        case f: FlowHandlerStrategy[Envelope] =>
          val flow: Flow[(Envelope, Envelope), (Done, Envelope), _] = f.flowCtx.asFlow
          Flow[(Offset, Envelope)]
            .map { case (_, env) => env -> env }
            .via(flow)
            .map { case (_, env) => sourceProvider.extractOffset(env) -> env }
      }

    if (afterEnvelopes == 1)
      // optimization of general AtLeastOnce case
      source.via(atLeastOnceHandlerFlow).mapAsync(1) {
        case (offset, envelope) =>
          reportProgress(saveOffset(projectionId, offset), envelope)
      }
    else
      source
        .via(atLeastOnceHandlerFlow)
        .groupedWithin(afterEnvelopes, orAfterDuration)
        .collect { case grouped if grouped.nonEmpty => grouped.last }
        .mapAsync(parallelism = 1) {
          case (offset, envelope) =>
            reportProgress(saveOffset(projectionId, offset), envelope)
        }
  }

  private def exactlyOnceProcessing(
      source: Source[(Offset, Envelope), NotUsed],
      recoveryStrategy: HandlerRecoveryStrategy): Source[Done, NotUsed] = {

    val handlerRecovery =
      HandlerRecoveryImpl[Offset, Envelope](projectionId, recoveryStrategy, logger, statusObserver)

    handlerStrategy match {
      case SingleHandlerStrategy(handler) =>
        source
          .mapAsync(1) {
            case (offset, env) =>
              handlerRecovery.applyRecovery(env, offset, offset, abort.future, () => handler.process(env))
          }

      case grouped: GroupedHandlerStrategy[Envelope] =>
        throw new IllegalStateException("not ported yet")

      case _: FlowHandlerStrategy[Envelope] =>
        // not possible, no API for this
        throw new IllegalStateException("Unsupported combination of exactlyOnce and flow")
    }
  }

  private def atMostOnceProcessing(
      source: Source[(Offset, Envelope), NotUsed],
      recoveryStrategy: HandlerRecoveryStrategy): Source[Done, NotUsed] = {

    val handlerRecovery =
      HandlerRecoveryImpl[Offset, Envelope](projectionId, recoveryStrategy, logger, statusObserver)

    handlerStrategy match {
      case SingleHandlerStrategy(handler) =>
        source
          .mapAsync(parallelism = 1) {
            case (offset, envelope) =>
              reportProgress(
                saveOffset(projectionId, offset)
                  .flatMap(_ =>
                    handlerRecovery
                      .applyRecovery(envelope, offset, offset, abort.future, () => handler.process(envelope))),
                envelope)
          }
          .map(_ => Done)
      case _ =>
        // not possible, no API for this
        throw new IllegalStateException("Unsupported combination of atMostOnce and grouped")
    }

  }

  def mappedSource(): Source[Done, _] = {

    statusObserver.started(projectionId)

    val source: Source[(Offset, Envelope), NotUsed] =
      Source
        .futureSource(
          handlerStrategy.lifecycle
            .tryStart()
            .flatMap(_ => sourceProvider.source(() => readOffsets())))
        .via(killSwitch.flow)
        .map(env => (sourceProvider.extractOffset(env), env))
        .filter {
          case (offset, _) =>
            sourceProvider.verifyOffset(offset) match {
              case VerificationSuccess => true
              case VerificationFailure(reason) =>
                logger.warning(
                  "Source provider instructed projection to skip offset [{}] with reason: {}",
                  offset,
                  reason)
                false
            }
        }
        .mapMaterializedValue(_ => NotUsed)

    val composedSource: Source[Done, NotUsed] =
      offsetStrategy match {
        case ExactlyOnce(recoveryStrategyOpt) =>
          exactlyOnceProcessing(source, recoveryStrategyOpt.getOrElse(settings.recoveryStrategy))

        case AtLeastOnce(afterEnvelopesOpt, orAfterDurationOpt, recoveryStrategyOpt) =>
          atLeastOnceProcessing(
            source,
            afterEnvelopesOpt.getOrElse(settings.saveOffsetAfterEnvelopes),
            orAfterDurationOpt.getOrElse(settings.saveOffsetAfterDuration),
            recoveryStrategyOpt.getOrElse(settings.recoveryStrategy))

        case AtMostOnce(recoveryStrategyOpt) =>
          atMostOnceProcessing(source, recoveryStrategyOpt.getOrElse(settings.recoveryStrategy))
      }

    RunningProjection.stopHandlerOnTermination(composedSource, projectionId, handlerStrategy.lifecycle, statusObserver)

  }
}
