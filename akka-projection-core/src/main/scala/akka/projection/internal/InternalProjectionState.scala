/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

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
import akka.projection.RunningProjection.AbortProjectionException
import akka.projection.StatusObserver
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.HandlerLifecycle
import akka.projection.scaladsl.SourceProvider
import akka.stream.KillSwitches
import akka.stream.SharedKillSwitch
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source

/**
 * INTERNAL API
 */
@InternalApi
private[akka] abstract class InternalProjectionState[Offset, Envelope](
    projectionId: ProjectionId,
    sourceProvider: SourceProvider[Offset, Envelope],
    offsetStrategy: OffsetStrategy,
    handlerStrategy: HandlerStrategy,
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

  /**
   * A convenience method to serialize asynchronous operations to occur one after another is complete
   */
  private def serialize(batches: Map[String, Seq[ProjectionContextImpl[Offset, Envelope]]])(
      op: (String, Seq[ProjectionContextImpl[Offset, Envelope]]) => Future[Done]): Future[Done] = {

    val logProgressEvery: Int = 5
    val size = batches.size
    logger.debug("Processing [{}] partitioned batches serially", size)

    def loop(remaining: List[(String, Seq[ProjectionContextImpl[Offset, Envelope]])], n: Int): Future[Done] = {
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
      source: Source[ProjectionContextImpl[Offset, Envelope], NotUsed],
      afterEnvelopes: Int,
      orAfterDuration: FiniteDuration,
      recoveryStrategy: HandlerRecoveryStrategy): Source[Done, NotUsed] = {

    val atLeastOnceHandlerFlow
        : Flow[ProjectionContextImpl[Offset, Envelope], ProjectionContextImpl[Offset, Envelope], NotUsed] =
      handlerStrategy match {
        case single: SingleHandlerStrategy[Envelope] =>
          val handler = single.handler()
          val handlerRecovery =
            HandlerRecoveryImpl[Offset, Envelope](projectionId, recoveryStrategy, logger, statusObserver)

          Flow[ProjectionContextImpl[Offset, Envelope]].mapAsync(parallelism = 1) { context =>
            handlerRecovery
              .applyRecovery(
                context.envelope,
                context.offset,
                context.offset,
                abort.future,
                () => handler.process(context.envelope))
              .map(_ => context)
          }

        case grouped: GroupedHandlerStrategy[Envelope] =>
          val groupAfterEnvelopes = grouped.afterEnvelopes.getOrElse(settings.groupAfterEnvelopes)
          val groupAfterDuration = grouped.orAfterDuration.getOrElse(settings.groupAfterDuration)
          val handler = grouped.handler()
          val handlerRecovery =
            HandlerRecoveryImpl[Offset, Envelope](projectionId, recoveryStrategy, logger, statusObserver)

          Flow[ProjectionContextImpl[Offset, Envelope]]
            .groupedWithin(groupAfterEnvelopes, groupAfterDuration)
            .filterNot(_.isEmpty)
            .mapAsync(parallelism = 1) { group =>
              val first = group.head
              val last = group.last
              val envelopes = group.map { _.envelope }
              handlerRecovery
                .applyRecovery(
                  first.envelope,
                  first.offset,
                  last.offset,
                  abort.future,
                  () => handler.process(envelopes))
                .map(_ => last)
            }

        case f: FlowHandlerStrategy[Envelope] =>
          val flow =
            f.flowCtx.asFlow
          Flow[ProjectionContextImpl[Offset, Envelope]]
            .map { context => context.envelope -> context }
            .via(flow)
            .map { case (_, context) => context.asInstanceOf[ProjectionContextImpl[Offset, Envelope]] }
      }

    if (afterEnvelopes == 1)
      // optimization of general AtLeastOnce case
      source.via(atLeastOnceHandlerFlow).mapAsync(1) { context =>
        reportProgress(saveOffset(projectionId, context.offset), context.envelope)
      }
    else
      source
        .via(atLeastOnceHandlerFlow)
        .groupedWithin(afterEnvelopes, orAfterDuration)
        .collect { case grouped if grouped.nonEmpty => grouped.last }
        .mapAsync(parallelism = 1) { context =>
          reportProgress(saveOffset(projectionId, context.offset), context.envelope)
        }
  }

  private def exactlyOnceProcessing(
      source: Source[ProjectionContextImpl[Offset, Envelope], NotUsed],
      recoveryStrategy: HandlerRecoveryStrategy): Source[Done, NotUsed] = {

    val handlerRecovery =
      HandlerRecoveryImpl[Offset, Envelope](projectionId, recoveryStrategy, logger, statusObserver)

    def processGrouped(
        handler: Handler[immutable.Seq[Envelope]],
        handlerRecovery: HandlerRecoveryImpl[Offset, Envelope],
        envelopesAndOffsets: immutable.Seq[ProjectionContextImpl[Offset, Envelope]]): Future[Done] = {

      def processEnvelopes(partitioned: immutable.Seq[ProjectionContextImpl[Offset, Envelope]]): Future[Done] = {
        val firstOffset = partitioned.head.offset
        val lastOffset = partitioned.last.offset
        val envelopes = partitioned.map { _.envelope }

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
          case ProjectionContextImpl(offset: MergeableOffset[_, _], _) =>
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
      case single: SingleHandlerStrategy[Envelope] =>
        val handler = single.handler()
        source
          .mapAsync(1) { context =>
            logger.warning(" Processing {}", context.envelope)
            val eventualDone: () => Future[Done] = () => {
              handler.process(context.envelope)
            }
            val processed =
              handlerRecovery.applyRecovery(
                context.envelope,
                context.offset,
                context.offset,
                abort.future,
                eventualDone)
            reportProgress(processed, context.envelope)
          }

      case grouped: GroupedHandlerStrategy[Envelope] =>
        val groupAfterEnvelopes = grouped.afterEnvelopes.getOrElse(settings.groupAfterEnvelopes)
        val groupAfterDuration = grouped.orAfterDuration.getOrElse(settings.groupAfterDuration)
        val handler = grouped.handler()

        source
          .groupedWithin(groupAfterEnvelopes, groupAfterDuration)
          .filterNot(_.isEmpty)
          .mapAsync(parallelism = 1) { group =>
            val last = group.last
            reportProgress(processGrouped(handler, handlerRecovery, group), last.envelope)
          }

      case _: FlowHandlerStrategy[_] =>
        // not possible, no API for this
        throw new IllegalStateException("Unsupported combination of exactlyOnce and flow")
    }
  }

  private def atMostOnceProcessing(
      source: Source[ProjectionContextImpl[Offset, Envelope], NotUsed],
      recoveryStrategy: HandlerRecoveryStrategy): Source[Done, NotUsed] = {

    val handlerRecovery =
      HandlerRecoveryImpl[Offset, Envelope](projectionId, recoveryStrategy, logger, statusObserver)

    handlerStrategy match {
      case single: SingleHandlerStrategy[Envelope] =>
        val handler = single.handler()
        source
          .mapAsync(parallelism = 1) { context =>
            val processed =
              saveOffset(projectionId, context.offset).flatMap { _ =>
                handlerRecovery
                  .applyRecovery(
                    context.envelope,
                    context.offset,
                    context.offset,
                    abort.future,
                    () => handler.process(context.envelope))
              }
            reportProgress(processed, context.envelope)
          }
          .map(_ => Done)
      case _ =>
        // not possible, no API for this
        throw new IllegalStateException("Unsupported combination of atMostOnce and grouped")
    }

  }

  def mappedSource(): Source[Done, _] = {

    val handlerLifecycle = handlerStrategy.lifecycle
    statusObserver.started(projectionId)

    val source: Source[ProjectionContextImpl[Offset, Envelope], NotUsed] =
      Source
        .futureSource(
          handlerLifecycle
            .tryStart()
            .flatMap(_ => sourceProvider.source(() => readOffsets())))
        .via(killSwitch.flow)
        .map(env => ProjectionContextImpl(sourceProvider.extractOffset(env), env))
        .filter { context =>
          sourceProvider.verifyOffset(context.offset) match {
            case VerificationSuccess => true
            case VerificationFailure(reason) =>
              logger.warning(
                "Source provider instructed projection to skip offset [{}] with reason: {}",
                context.offset,
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

    stopHandlerOnTermination(composedSource, handlerLifecycle)

  }

  /**
   * Adds a `watchTermination` on the passed Source that will call the `stopHandler` on completion.
   *
   * The stopHandler function is called on success or failure. In case of failure, the original failure is preserved.
   */
  private def stopHandlerOnTermination(
      src: Source[Done, NotUsed],
      handlerLifecycle: HandlerLifecycle): Source[Done, Future[Done]] = {
    src.watchTermination() { (_, futDone) =>
      handlerStrategy.recreateHandlerOnNextAccess()
      futDone
        .andThen(_ => handlerLifecycle.tryStop())
        .andThen {
          case Success(_) =>
            statusObserver.stopped(projectionId)
          case Failure(AbortProjectionException) =>
            statusObserver.stopped(projectionId) // no restart
          case Failure(exc) =>
            Try(statusObserver.stopped(projectionId))
            statusObserver.failed(projectionId, exc)
        }
    }
  }
}
