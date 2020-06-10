/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import akka.Done
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.event.LoggingAdapter
import akka.projection.HandlerRecoveryStrategy
import akka.projection.MergeableKey
import akka.projection.MergeableOffset
import akka.projection.OffsetVerification.VerificationFailure
import akka.projection.OffsetVerification.VerificationSuccess
import akka.projection.ProjectionId
import akka.projection.RunningProjection
import akka.projection.StatusObserver
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.SourceProvider
import akka.stream.KillSwitches
import akka.stream.SharedKillSwitch
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success
import scala.util.control.NonFatal

/**
 * INTERNAL API
 */
@InternalApi
private[akka] abstract class InternalProjectionState[Offset, Envelope](
    projectionId: ProjectionId,
    sourceProvider: SourceProvider[Offset, Envelope],
    offsetStrategy: OffsetStrategy,
    handlerStrategy: HandlerStrategy[Envelope],
    statusObserver: StatusObserver[Envelope],
    settings: ProjectionSettings) {

  def logger: LoggingAdapter
  implicit def system: ActorSystem[_]
  implicit def executionContext: ExecutionContext

  var telemetry: Telemetry = null

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

  /**
   * A convenience method to serialize asynchronous operations to occur one after another is complete
   */
  private def serialize(batches: Map[String, Seq[(Offset, Envelope)]])(
      op: (String, Seq[(Offset, Envelope)]) => Future[Done]): Future[Done] = {

    val logProgressEvery: Int = 5
    val size = batches.size
    logger.debug("Processing [{}] partitioned batches serially", size)

    def loop(remaining: List[(String, Seq[(Offset, Envelope)])], n: Int): Future[Done] = {
      remaining match {
        case Nil => Future.successful(Done)
        case (key, batch) :: tail =>
          op(key, batch).flatMap { _ =>
            if (n % logProgressEvery == 0)
              logger.debug("Processed batches [{}] of [{}]", n, size)
            loop(tail, n + 1)
          }
      }
    }

    val result = loop(batches.toList, n = 1)

    result.onComplete {
      case Success(_) =>
        logger.debug("Processing completed of [{}] batches", size)
      case Failure(e) =>
        logger.error(e, "Processing of batches failed")
    }

    result

  }

  private def atLeastOnceProcessing(
      source: Source[(Offset, Envelope), NotUsed],
      afterEnvelopes: Int,
      orAfterDuration: FiniteDuration,
      recoveryStrategy: HandlerRecoveryStrategy): Source[Done, NotUsed] = {

    val atLeastOnceHandlerFlow: Flow[(Offset, Envelope), (Offset, Envelope), NotUsed] =
      handlerStrategy match {
        case SingleHandlerStrategy(handler) =>
          val handlerRecovery =
            HandlerRecoveryImpl[Offset, Envelope](projectionId, recoveryStrategy, logger, statusObserver)

          Flow[(Offset, Envelope)]
            .mapAsync(parallelism = 1) {
              case elem @ (offset, envelope) =>
                telemetry
                  .map(_.onEnvelopeReady(projectionId, offset, envelope))
                  .map { _ =>
                    handlerRecovery
                      .applyRecovery(envelope, offset, offset, abort.future, () => handler.process(envelope))
                      .map(_ => elem)
                  }
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

    def processGrouped(
        handler: Handler[immutable.Seq[Envelope]],
        handlerRecovery: HandlerRecoveryImpl[Offset, Envelope],
        envelopesAndOffsets: immutable.Seq[(Offset, Envelope)]): Future[Done] = {

      def processEnvelopes(partitioned: immutable.Seq[(Offset, Envelope)]): Future[Done] = {
        val (firstOffset, _) = partitioned.head
        val (lastOffset, _) = partitioned.last
        val envelopes = partitioned.map { case (_, env) => env }

        handlerRecovery.applyRecovery(
          envelopes.head,
          firstOffset,
          lastOffset,
          abort.future,
          () => handler.process(envelopes))
      }

      // FIXME create a SourceProvider trait that implies mergeable offsets?
      if (sourceProvider.isOffsetMergeable) {
        val batches = envelopesAndOffsets.groupBy {
          // FIXME matched source provider should always be MergeableOffset
          case (offset: MergeableOffset[_, _], _) =>
            // FIXME we can assume there's only one actual offset per envelope, but there should be a better way to represent this
            val mergeableKey = offset.entries.head._1.asInstanceOf[MergeableKey]
            mergeableKey.surrogateKey
          case _ =>
            // should never happen
            throw new IllegalStateException("The offset should always be of type MergeableOffset")
        }

        // process batches in sequence, but not concurrently, in order to provide singled threaded guarantees
        // to the user envelope handler
        serialize(batches) { (surrogateKey, partitionedEnvelopes) =>
          logger.debug("Processing grouped envelopes for MergeableOffset with key [{}]", surrogateKey)
          processEnvelopes(partitionedEnvelopes)
        }

      } else {
        processEnvelopes(envelopesAndOffsets)
      }

    }

    handlerStrategy match {
      case SingleHandlerStrategy(handler) =>
        source
          .mapAsync(1) {
            case (offset, env) =>
              val processed =
                handlerRecovery.applyRecovery(env, offset, offset, abort.future, () => handler.process(env))
              reportProgress(processed, env)
          }

      case grouped: GroupedHandlerStrategy[Envelope] =>
        val groupAfterEnvelopes = grouped.afterEnvelopes.getOrElse(settings.groupAfterEnvelopes)
        val groupAfterDuration = grouped.orAfterDuration.getOrElse(settings.groupAfterDuration)

        source
          .groupedWithin(groupAfterEnvelopes, groupAfterDuration)
          .filterNot(_.isEmpty)
          .mapAsync(parallelism = 1) { group =>
            val (_, lastEnvelope) = group.last
            reportProgress(processGrouped(grouped.handler, handlerRecovery, group), lastEnvelope)
          }

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
              val processed =
                saveOffset(projectionId, offset).flatMap { _ =>
                  handlerRecovery
                    .applyRecovery(envelope, offset, offset, abort.future, () => handler.process(envelope))
                }
              reportProgress(processed, envelope)
          }
          .map(_ => Done)
      case _ =>
        // not possible, no API for this
        throw new IllegalStateException("Unsupported combination of atMostOnce and grouped")
    }

  }

  def mappedSource(): Source[Done, _] = {

    statusObserver.started(projectionId)
    telemetry = TelemetryProvider.started(projectionId)

    val source: Source[(Offset, Envelope), NotUsed] =
      Source
        .futureSource(
          handlerStrategy.lifecycle
            .tryStart()
            .flatMap(_ => sourceProvider.source(() => readOffsets())))
        .via(killSwitch.flow)
        .map { env =>
          val offset = sourceProvider.extractOffset(env)

          val extraInfo = telemetry.onEnvelopeReady(projectionId, offset, env)
          // TODO: add the extraInfo as context on the following tuple.
          (offset, env)
        }
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
