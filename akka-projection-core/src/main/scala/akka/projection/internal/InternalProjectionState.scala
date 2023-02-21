/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
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
import akka.projection.MergeableOffset
import akka.projection.OffsetVerification.VerificationFailure
import akka.projection.OffsetVerification.VerificationSuccess
import akka.projection.ProjectionId
import akka.projection.RunningProjection.AbortProjectionException
import akka.projection.StatusObserver
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.HandlerLifecycle
import akka.projection.scaladsl.MergeableOffsetSourceProvider
import akka.projection.scaladsl.SourceProvider
import akka.projection.scaladsl.VerifiableSourceProvider
import akka.stream.KillSwitches
import akka.stream.SharedKillSwitch
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source

/**
 * INTERNAL API
 */
@InternalApi
private[projection] abstract class InternalProjectionState[Offset, Envelope](
    projectionId: ProjectionId,
    sourceProvider: SourceProvider[Offset, Envelope],
    offsetStrategy: OffsetStrategy,
    handlerStrategy: HandlerStrategy,
    statusObserver: StatusObserver[Envelope],
    settings: ProjectionSettings) {

  def logger: LoggingAdapter
  implicit def system: ActorSystem[_]
  implicit def executionContext: ExecutionContext

  private var telemetry: Telemetry = NoopTelemetry
  private[projection] def getTelemetry() = telemetry

  def readPaused(): Future[Boolean]
  def readOffsets(): Future[Option[Offset]]
  def saveOffset(projectionId: ProjectionId, offset: Offset): Future[Done]
  val killSwitch: SharedKillSwitch = KillSwitches.shared(projectionId.id)
  val abort: Promise[Done] = Promise()

  protected def saveOffsetAndReport(
      projectionId: ProjectionId,
      projectionContext: ProjectionContextImpl[Offset, Envelope],
      batchSize: Int): Future[Done] = {
    saveOffset(projectionId, projectionContext.offset)
      .map { done =>
        try {
          statusObserver.offsetProgress(projectionId, projectionContext.envelope)
        } catch {
          case NonFatal(_) => // ignore
        }
        getTelemetry().onOffsetStored(batchSize)
        done
      }
  }

  protected def saveOffsetsAndReport(
      projectionId: ProjectionId,
      batch: immutable.Seq[ProjectionContextImpl[Offset, Envelope]]): Future[Done] = {

    // The batch contains multiple projections contexts. Each of these contexts may represent
    // a single envelope or a group of envelopes. The size of the batch and the size of the
    // group may differ.
    val totalNumberOfEnvelopes = batch.map { _.groupSize }.sum
    val last = batch.last

    saveOffsetAndReport(projectionId, last, totalNumberOfEnvelopes)
  }

  /**
   * A convenience method to serialize asynchronous operations to occur one after another is complete
   */
  private def serialize(batches: Map[String, immutable.Seq[ProjectionContextImpl[Offset, Envelope]]])(
      op: (String, immutable.Seq[ProjectionContextImpl[Offset, Envelope]]) => Future[Done]): Future[Done] = {

    val logProgressEvery: Int = 5
    val size = batches.size
    logger.debug("Processing [{}] partitioned batches serially", size)

    def loop(
        remaining: List[(String, immutable.Seq[ProjectionContextImpl[Offset, Envelope]])],
        n: Int): Future[Done] = {
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
        case single: SingleHandlerStrategy[Envelope] @unchecked =>
          val handler = single.handler()
          val handlerRecovery =
            HandlerRecoveryImpl[Offset, Envelope](projectionId, recoveryStrategy, logger, statusObserver, telemetry)

          Flow[ProjectionContextImpl[Offset, Envelope]].mapAsync(parallelism = 1) { context =>
            val measured: () => Future[Done] = { () =>
              handler.process(context.envelope).map { done =>
                statusObserver.afterProcess(projectionId, context.envelope)
                // `telemetry.afterProcess` is invoked immediately after `handler.process`
                telemetry.afterProcess(context.externalContext)
                done
              }
            }
            handlerRecovery
              .applyRecovery(context.envelope, context.offset, context.offset, abort.future, measured)
              .map { _ =>
                context
              }

          }

        case grouped: GroupedHandlerStrategy[Envelope] @unchecked =>
          val groupAfterEnvelopes = grouped.afterEnvelopes.getOrElse(settings.groupAfterEnvelopes)
          val groupAfterDuration = grouped.orAfterDuration.getOrElse(settings.groupAfterDuration)
          val handler = grouped.handler()
          val handlerRecovery =
            HandlerRecoveryImpl[Offset, Envelope](projectionId, recoveryStrategy, logger, statusObserver, telemetry)

          Flow[ProjectionContextImpl[Offset, Envelope]]
            .groupedWithin(groupAfterEnvelopes, groupAfterDuration)
            .filterNot(_.isEmpty)
            .mapAsync(parallelism = 1) { group =>
              val first = group.head
              val last = group.last
              val envelopes = group.map { _.envelope }
              val measured: () => Future[Done] = { () =>
                handler.process(envelopes).map { done =>
                  group.foreach { ctx =>
                    statusObserver.afterProcess(projectionId, ctx.envelope)
                    telemetry.afterProcess(ctx.externalContext)
                  }
                  done
                }
              }
              handlerRecovery
                .applyRecovery(first.envelope, first.offset, last.offset, abort.future, measured)
                .map { _ =>
                  last.withGroupSize(envelopes.length)
                }
            }

        case f: FlowHandlerStrategy[Envelope] @unchecked =>
          val flow =
            f.flowCtx.asFlow.watchTermination() {
              case (_, futDone) =>
                futDone.recoverWith {
                  case t =>
                    telemetry.error(t)
                    futDone
                }
            }
          Flow[ProjectionContextImpl[Offset, Envelope]]
            .map { context => context.envelope -> context }
            .via(flow)
            .map {
              case (_, context) =>
                val ctx = context.asInstanceOf[ProjectionContextImpl[Offset, Envelope]]
                statusObserver.afterProcess(projectionId, ctx.envelope)
                telemetry.afterProcess(ctx.externalContext)
                ctx
            }
      }

    if (afterEnvelopes == 1)
      // optimization of general AtLeastOnce case
      source.via(atLeastOnceHandlerFlow).mapAsync(1) { context =>
        saveOffsetAndReport(projectionId, context, context.groupSize)
      }
    else {
      source
        .via(atLeastOnceHandlerFlow)
        .groupedWithin(afterEnvelopes, orAfterDuration)
        .filterNot(_.isEmpty)
        .mapAsync(parallelism = 1) { batch =>
          saveOffsetsAndReport(projectionId, batch)
        }
    }
  }

  private def exactlyOnceProcessing(
      source: Source[ProjectionContextImpl[Offset, Envelope], NotUsed],
      recoveryStrategy: HandlerRecoveryStrategy): Source[Done, NotUsed] = {

    val handlerRecovery =
      HandlerRecoveryImpl[Offset, Envelope](projectionId, recoveryStrategy, logger, statusObserver, telemetry)

    def processGrouped(
        handler: Handler[immutable.Seq[Envelope]],
        handlerRecovery: HandlerRecoveryImpl[Offset, Envelope],
        envelopesAndOffsets: immutable.Seq[ProjectionContextImpl[Offset, Envelope]]): Future[Done] = {

      def processEnvelopes(partitioned: immutable.Seq[ProjectionContextImpl[Offset, Envelope]]): Future[Done] = {
        val first = partitioned.head
        val firstOffset = first.offset
        val lastOffset = partitioned.last.offset
        val envelopes = partitioned.map {
          _.envelope
        }

        val measured: () => Future[Done] = { () =>
          handler.process(envelopes).map { done =>
            partitioned.foreach { ctx =>
              statusObserver.afterProcess(projectionId, ctx.envelope)
              telemetry.afterProcess(ctx.externalContext)
            }
            done
          }
        }
        val onSkip = () => saveOffsetsAndReport(projectionId, partitioned)
        handlerRecovery.applyRecovery(first.envelope, firstOffset, lastOffset, abort.future, measured, onSkip)
      }

      sourceProvider match {
        case _: MergeableOffsetSourceProvider[_, _] =>
          val batches = envelopesAndOffsets
            .flatMap {
              case context @ ProjectionContextImpl(offset: MergeableOffset[_] @unchecked, _, _, _) =>
                offset.entries.toSeq.map {
                  case (key, _) => (key, context)
                }
              case _ =>
                // should never happen
                throw new IllegalStateException("The offset should always be of type MergeableOffset")
            }
            .groupBy { case (key, _) => key }
            .map {
              case (key, keyAndContexts) =>
                val envs = keyAndContexts.map {
                  case (_, context) => context.asInstanceOf[ProjectionContextImpl[Offset, Envelope]]
                }
                key -> envs
            }

          // process batches in sequence, but not concurrently, in order to provide singled threaded guarantees
          // to the user envelope handler
          serialize(batches) {
            case (surrogateKey, partitionedEnvelopes) =>
              logger.debug("Processing grouped envelopes for MergeableOffset with key [{}]", surrogateKey)
              processEnvelopes(partitionedEnvelopes)
          }
        case _ =>
          processEnvelopes(envelopesAndOffsets)
      }
    }

    handlerStrategy match {
      case single: SingleHandlerStrategy[Envelope] @unchecked =>
        val handler: Handler[Envelope] = single.handler()
        source
          .mapAsync(1) { context =>
            val measured: () => Future[Done] = () => {
              handler.process(context.envelope).map { done =>
                statusObserver.afterProcess(projectionId, context.envelope)
                // `telemetry.afterProcess` is invoked immediately after `handler.process`
                telemetry.afterProcess(context.externalContext)

                try {
                  statusObserver.offsetProgress(projectionId, context.envelope)
                } catch {
                  case NonFatal(_) => // ignore
                }
                telemetry.onOffsetStored(1)

                done
              }
            }

            val onSkip = () => saveOffsetAndReport(projectionId, context, 1)
            handlerRecovery.applyRecovery(
              context.envelope,
              context.offset,
              context.offset,
              abort.future,
              measured,
              onSkip)

          }

      case grouped: GroupedHandlerStrategy[Envelope] @unchecked =>
        val groupAfterEnvelopes = grouped.afterEnvelopes.getOrElse(settings.groupAfterEnvelopes)
        val groupAfterDuration = grouped.orAfterDuration.getOrElse(settings.groupAfterDuration)
        val handler = grouped.handler()

        source
          .groupedWithin(groupAfterEnvelopes, groupAfterDuration)
          .filterNot(_.isEmpty)
          .mapAsync(parallelism = 1) { group =>
            val last: ProjectionContextImpl[Offset, Envelope] = group.last
            processGrouped(handler, handlerRecovery, group)
              .map { t =>
                try {
                  statusObserver.offsetProgress(projectionId, last.envelope)
                } catch {
                  case NonFatal(_) => // ignore
                }
                telemetry.onOffsetStored(group.length)
                t
              }
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
      HandlerRecoveryImpl[Offset, Envelope](projectionId, recoveryStrategy, logger, statusObserver, telemetry)

    handlerStrategy match {
      case single: SingleHandlerStrategy[Envelope] @unchecked =>
        val handler = single.handler()
        source
          .mapAsync(parallelism = 1) { context =>
            saveOffsetAndReport(projectionId, context, 1).flatMap { _ =>
              val measured: () => Future[Done] = { () =>
                handler.process(context.envelope).map { done =>
                  statusObserver.afterProcess(projectionId, context.envelope)
                  // `telemetry.afterProcess` is invoked immediately after `handler.process`
                  telemetry.afterProcess(context.externalContext)
                  done
                }
              }

              handlerRecovery
                .applyRecovery(context.envelope, context.offset, context.offset, abort.future, measured)
            }
          }
          .map(_ => Done)
      case _ =>
        // not possible, no API for this
        throw new IllegalStateException("Unsupported combination of atMostOnce and grouped")
    }

  }

  def mappedSource(): Source[Done, Future[Done]] = {
    val handlerLifecycle = handlerStrategy.lifecycle
    statusObserver.started(projectionId)
    telemetry = TelemetryProvider.start(projectionId, system)

    val source: Source[ProjectionContextImpl[Offset, Envelope], NotUsed] =
      Source
        .futureSource(readPaused().flatMap {
          case false =>
            logger.debug("Projection [{}] started in resumed mode.", projectionId)
            handlerLifecycle
              .tryStart()
              .flatMap { _ =>
                sourceProvider
                  .source(() => readOffsets())
              }
          case true =>
            logger.info("Projection [{}] started in paused mode.", projectionId)
            // paused stream, no elements
            Future.successful(Source.never[Envelope])
        })
        .via(killSwitch.flow)
        .map { env =>
          statusObserver.beforeProcess(projectionId, env)
          val externalContext = telemetry.beforeProcess(env, sourceProvider.extractCreationTime(env))
          ProjectionContextImpl(sourceProvider.extractOffset(env), env, externalContext)
        }
        .filter { context =>
          sourceProvider match {
            case vsp: VerifiableSourceProvider[Offset, Envelope] =>
              vsp.verifyOffset(context.offset) match {
                case VerificationSuccess => true
                case VerificationFailure(reason) =>
                  logger.warning(
                    "Source provider instructed projection to skip offset [{}] with reason: {}",
                    context.offset,
                    reason)
                  false
              }
            case _ => true
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
    src
      .watchTermination() { (_, futDone) =>
        handlerStrategy.recreateHandlerOnNextAccess()
        futDone
          .andThen { case _ => handlerLifecycle.tryStop() }
          .andThen {
            case Success(_) =>
              telemetry.stopped()
              statusObserver.stopped(projectionId)
            case Failure(AbortProjectionException) =>
              telemetry.stopped()
              statusObserver.stopped(projectionId) // no restart
            case Failure(exc) =>
              // For observer and telemetry, invoke failed first and stopped second.
              Try(telemetry.failed(exc))
              Try(telemetry.stopped())
              Try(statusObserver.failed(projectionId, exc))
              Try(statusObserver.stopped(projectionId))
          }
      }
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] final case class ManagementState(paused: Boolean)
