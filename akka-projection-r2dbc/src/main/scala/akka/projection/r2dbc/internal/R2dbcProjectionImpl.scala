/*
 * Copyright (C) 2022-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc.internal

import java.util.concurrent.atomic.AtomicLong

import scala.annotation.nowarn
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.event.Logging
import akka.event.LoggingAdapter
import akka.persistence.query.DeletedDurableState
import akka.persistence.query.DurableStateChange
import akka.persistence.query.UpdatedDurableState
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.LoadEventQuery
import akka.persistence.r2dbc.internal.EnvelopeOrigin
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.state.scaladsl.DurableStateStore
import akka.persistence.state.scaladsl.GetObjectResult
import akka.projection.BySlicesSourceProvider
import akka.projection.HandlerRecoveryStrategy
import akka.projection.HandlerRecoveryStrategy.Internal.RetryAndSkip
import akka.projection.HandlerRecoveryStrategy.Internal.Skip
import akka.projection.ProjectionContext
import akka.projection.ProjectionId
import akka.projection.RunningProjection
import akka.projection.RunningProjection.AbortProjectionException
import akka.projection.RunningProjectionManagement
import akka.projection.StatusObserver
import akka.projection.eventsourced.scaladsl.EventSourcedProvider.LoadEventsByPersistenceIdSourceProvider
import akka.projection.internal.ActorHandlerInit
import akka.projection.internal.AtLeastOnce
import akka.projection.internal.AtMostOnce
import akka.projection.internal.CanTriggerReplay
import akka.projection.internal.ExactlyOnce
import akka.projection.internal.GroupedHandlerStrategy
import akka.projection.internal.HandlerStrategy
import akka.projection.internal.InternalProjection
import akka.projection.internal.InternalProjectionState
import akka.projection.internal.JavaToScalaBySliceSourceProviderAdapter
import akka.projection.internal.ManagementState
import akka.projection.internal.OffsetStoredByHandler
import akka.projection.internal.OffsetStrategy
import akka.projection.internal.ProjectionContextImpl
import akka.projection.internal.ProjectionSettings
import akka.projection.internal.SettingsImpl
import akka.projection.javadsl
import akka.projection.r2dbc.R2dbcProjectionSettings
import akka.projection.r2dbc.internal.R2dbcOffsetStore.RejectedEnvelope
import akka.projection.r2dbc.internal.R2dbcProjectionImpl.extractOffsetPidSeqNr
import akka.projection.r2dbc.scaladsl.R2dbcHandler
import akka.projection.r2dbc.scaladsl.R2dbcSession
import akka.projection.scaladsl
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.SourceProvider
import akka.stream.RestartSettings
import akka.stream.scaladsl.FlowWithContext
import akka.stream.scaladsl.Source
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * INTERNAL API
 */
@InternalApi
private[projection] object R2dbcProjectionImpl {
  import akka.persistence.r2dbc.internal.EnvelopeOrigin.fromBacktracking
  import akka.persistence.r2dbc.internal.EnvelopeOrigin.isFilteredEvent

  val log: Logger = LoggerFactory.getLogger(classOf[R2dbcProjectionImpl[_, _]])

  private val FutureDone: Future[Done] = Future.successful(Done)
  private val FutureFalse: Future[Boolean] = Future.successful(false)

  private[projection] def createOffsetStore(
      projectionId: ProjectionId,
      sourceProvider: Option[BySlicesSourceProvider],
      settings: R2dbcProjectionSettings,
      r2dbcExecutor: R2dbcExecutor)(implicit system: ActorSystem[_]) = {
    new R2dbcOffsetStore(projectionId, sourceProvider, system, settings, r2dbcExecutor)
  }

  private val loadEnvelopeCounter = new AtomicLong
  private val replayRejectedCounter = new AtomicLong

  def loadEnvelope[Envelope](env: Envelope, sourceProvider: SourceProvider[_, Envelope])(
      implicit
      ec: ExecutionContext): Future[Envelope] = {
    env match {
      case eventEnvelope: EventEnvelope[_]
          if fromBacktracking(eventEnvelope) && eventEnvelope.eventOption.isEmpty && !eventEnvelope.filtered =>
        val pid = eventEnvelope.persistenceId
        val seqNr = eventEnvelope.sequenceNr
        (sourceProvider match {
          case loadEventQuery: LoadEventQuery =>
            loadEventQuery.loadEnvelope[Any](pid, seqNr)
          case loadEventQuery: akka.persistence.query.typed.javadsl.LoadEventQuery =>
            import scala.jdk.FutureConverters._
            loadEventQuery.loadEnvelope[Any](pid, seqNr).asScala
          case _ =>
            throw new IllegalArgumentException(
              s"Expected sourceProvider [${sourceProvider.getClass.getName}] " +
              "to implement LoadEventQuery when used with eventsBySlices.")
        }).map { loadedEnv =>
          val count = loadEnvelopeCounter.incrementAndGet()
          if (count % 1000 == 0)
            log.info("Loaded event lazily, persistenceId [{}], seqNr [{}]. Load count [{}]", pid, seqNr, count)
          else
            log.debug("Loaded event lazily, persistenceId [{}], seqNr [{}]. Load count [{}]", pid, seqNr, count)
          loadedEnv.asInstanceOf[Envelope]
        }

      case upd: UpdatedDurableState[_] if upd.value == null =>
        val pid = upd.persistenceId
        (sourceProvider match {
          case store: DurableStateStore[_] =>
            store.getObject(pid)
          case store: akka.persistence.state.javadsl.DurableStateStore[_] =>
            import scala.jdk.FutureConverters._
            store.getObject(pid).asScala.map(_.toScala)
          case unknown =>
            throw new IllegalArgumentException(s"Unsupported source provider type '${unknown.getClass}'")
        }).map {
          case GetObjectResult(Some(loadedValue), loadedRevision) =>
            val count = loadEnvelopeCounter.incrementAndGet()
            if (count % 1000 == 0)
              log.info(
                "Loaded durable state lazily, persistenceId [{}], revision [{}]. Load count [{}]",
                pid,
                loadedRevision,
                count)
            else
              log.debug(
                "Loaded durable state lazily, persistenceId [{}], revision [{}]. Load count [{}]",
                pid,
                loadedRevision,
                count)
            new UpdatedDurableState(pid, loadedRevision, loadedValue, upd.offset, upd.timestamp)
              .asInstanceOf[Envelope]
          case GetObjectResult(None, loadedRevision) =>
            new DeletedDurableState(pid, loadedRevision, upd.offset, upd.timestamp)
              .asInstanceOf[Envelope]
        }

      case _ =>
        Future.successful(env)
    }
  }

  private def extractOffsetPidSeqNr[Offset, Envelope](
      sourceProvider: SourceProvider[Offset, Envelope],
      envelope: Envelope): OffsetPidSeqNr =
    extractOffsetPidSeqNr(sourceProvider.extractOffset(envelope), envelope)

  private def extractOffsetPidSeqNr[Offset, Envelope](offset: Offset, envelope: Envelope): OffsetPidSeqNr = {
    // we could define a new trait for the SourceProvider to implement this in case other (custom) envelope types are needed
    envelope match {
      case env: EventEnvelope[_]         => OffsetPidSeqNr(offset, env.persistenceId, env.sequenceNr)
      case chg: UpdatedDurableState[_]   => OffsetPidSeqNr(offset, chg.persistenceId, chg.revision)
      case del: DeletedDurableState[_]   => OffsetPidSeqNr(offset, del.persistenceId, del.revision)
      case change: DurableStateChange[_] =>
        // in case additional types are added
        throw new IllegalArgumentException(
          s"DurableStateChange [${change.getClass.getName}] not implemented yet. Please report bug at https://github.com/akka/akka-projection/issues")
      case _ => OffsetPidSeqNr(offset)
    }
  }

  private[projection] def adaptedHandlerForExactlyOnce[Offset, Envelope](
      sourceProvider: SourceProvider[Offset, Envelope],
      handlerFactory: () => R2dbcHandler[Envelope],
      offsetStore: R2dbcOffsetStore,
      r2dbcExecutor: R2dbcExecutor)(implicit ec: ExecutionContext, system: ActorSystem[_]): () => Handler[Envelope] = {
    () =>

      new AdaptedR2dbcHandler(handlerFactory()) {
        override def process(envelope: Envelope): Future[Done] = {
          import R2dbcOffsetStore.Validation._
          offsetStore
            .validate(envelope)
            .flatMap {
              case Accepted =>
                if (isFilteredEvent(envelope)) {
                  val offset = extractOffsetPidSeqNr(sourceProvider, envelope)
                  offsetStore.saveOffset(offset)
                } else {
                  loadEnvelope(envelope, sourceProvider).flatMap { loadedEnvelope =>
                    val offset = extractOffsetPidSeqNr(sourceProvider, loadedEnvelope)
                    r2dbcExecutor.withConnection("exactly-once handler") { conn =>
                      // run users handler
                      val session = new R2dbcSession(conn)
                      delegate
                        .process(session, loadedEnvelope)
                        .flatMap { _ =>
                          offsetStore.saveOffsetInTx(conn, offset)
                        }
                    }
                  }
                }
              case Duplicate =>
                FutureDone
              case RejectedSeqNr =>
                replay(envelope).map(_ => Done)(ExecutionContext.parasitic)
              case RejectedBacktrackingSeqNr =>
                replay(envelope).map {
                  case true  => Done
                  case false => throwRejectedEnvelope(sourceProvider, envelope)
                }
            }
        }

        private def replay(originalEnvelope: Envelope): Future[Boolean] = {
          replayIfPossible(offsetStore, sourceProvider, originalEnvelope) { envelope =>
            val offset = extractOffsetPidSeqNr(sourceProvider, envelope.asInstanceOf[Envelope])
            if (isFilteredEvent(envelope)) {
              offsetStore.saveOffset(offset)
            } else {
              r2dbcExecutor.withConnection("exactly-once handler") { conn =>
                // run users handler
                val session = new R2dbcSession(conn)
                delegate
                  .process(session, envelope.asInstanceOf[Envelope])
                  .flatMap { _ =>
                    offsetStore.saveOffsetInTx(conn, offset)
                  }
              }
            }
          }
        }
      }
  }

  private[projection] def adaptedHandlerForGrouped[Offset, Envelope](
      sourceProvider: SourceProvider[Offset, Envelope],
      handlerFactory: () => R2dbcHandler[immutable.Seq[Envelope]],
      offsetStore: R2dbcOffsetStore,
      r2dbcExecutor: R2dbcExecutor)(
      implicit
      ec: ExecutionContext,
      system: ActorSystem[_]): () => Handler[immutable.Seq[Envelope]] = { () =>

    new AdaptedR2dbcHandler(handlerFactory()) {
      override def process(envelopes: Seq[Envelope]): Future[Done] = {
        import R2dbcOffsetStore.Validation._
        offsetStore.validateAll(envelopes).flatMap { isAcceptedEnvelopes =>
          // For simplicity we process the replayed envelopes one by one (group of 1), and also store the
          // offset for each separately.
          // Important to replay them sequentially to avoid concurrent processing and offset storage.
          val replayDone =
            isAcceptedEnvelopes.foldLeft(FutureDone) {
              case (previous, (env, RejectedSeqNr)) =>
                previous.flatMap { _ =>
                  replay(env).map(_ => Done)(ExecutionContext.parasitic)
                }
              case (previous, (env, RejectedBacktrackingSeqNr)) =>
                previous.flatMap { _ =>
                  replay(env).map {
                    case true  => Done
                    case false => throwRejectedEnvelope(sourceProvider, env)
                  }
                }
              case (previous, _) =>
                previous
            }

          replayDone.flatMap { _ =>
            def processAcceptedEnvelopes(envelopes: Seq[Envelope]): Future[Done] = {
              if (envelopes.isEmpty) {
                FutureDone
              } else {
                Future.sequence(envelopes.map(env => loadEnvelope(env, sourceProvider))).flatMap { loadedEnvelopes =>
                  val offsets = loadedEnvelopes.iterator.map(extractOffsetPidSeqNr(sourceProvider, _)).toVector
                  val filteredEnvelopes = loadedEnvelopes.filterNot(isFilteredEvent)
                  if (filteredEnvelopes.isEmpty) {
                    offsetStore.saveOffsets(offsets)
                  } else {
                    r2dbcExecutor.withConnection("grouped handler") { conn =>
                      // run users handler
                      val session = new R2dbcSession(conn)
                      delegate.process(session, filteredEnvelopes).flatMap { _ =>
                        offsetStore.saveOffsetsInTx(conn, offsets)
                      }
                    }
                  }
                }
              }
            }

            val acceptedEnvelopes = isAcceptedEnvelopes.collect {
              case (env, Accepted) =>
                env
            }
            val hasRejected =
              isAcceptedEnvelopes.exists {
                case (_, RejectedSeqNr)             => true
                case (_, RejectedBacktrackingSeqNr) => true
                case _                              => false
              }

            if (hasRejected) {
              // need second validation after replay to remove duplicates
              offsetStore
                .validateAll(acceptedEnvelopes)
                .flatMap { isAcceptedEnvelopes2 =>
                  val acceptedEnvelopes2 = isAcceptedEnvelopes2.collect {
                    case (env, Accepted) => env
                  }
                  processAcceptedEnvelopes(acceptedEnvelopes2)
                }
            } else {
              processAcceptedEnvelopes(acceptedEnvelopes)
            }
          }
        }
      }

      private def replay(originalEnvelope: Envelope): Future[Boolean] = {
        replayIfPossible(offsetStore, sourceProvider, originalEnvelope) { envelope =>
          val offset = extractOffsetPidSeqNr(sourceProvider, envelope.asInstanceOf[Envelope])
          if (isFilteredEvent(envelope)) {
            offsetStore.saveOffset(offset)
          } else {
            r2dbcExecutor.withConnection("grouped handler") { conn =>
              // run users handler
              val session = new R2dbcSession(conn)
              delegate.process(session, Seq(envelope.asInstanceOf[Envelope])).flatMap { _ =>
                offsetStore.saveOffsetInTx(conn, offset)
              }
            }
          }
        }
      }
    }
  }

  private[projection] def adaptedHandlerForAtLeastOnce[Offset, Envelope](
      sourceProvider: SourceProvider[Offset, Envelope],
      handlerFactory: () => R2dbcHandler[Envelope],
      offsetStore: R2dbcOffsetStore,
      r2dbcExecutor: R2dbcExecutor)(implicit ec: ExecutionContext, system: ActorSystem[_]): () => Handler[Envelope] = {
    () =>
      new AdaptedR2dbcHandler(handlerFactory()) {
        override def process(envelope: Envelope): Future[Done] = {
          import R2dbcOffsetStore.Validation._
          offsetStore
            .validate(envelope)
            .flatMap {
              case Accepted =>
                if (isFilteredEvent(envelope)) {
                  offsetStore.addInflight(envelope)
                  FutureDone
                } else {
                  loadEnvelope(envelope, sourceProvider).flatMap { loadedEnvelope =>
                    r2dbcExecutor
                      .withConnection("at-least-once handler") { conn =>
                        // run users handler
                        val session = new R2dbcSession(conn)
                        delegate.process(session, loadedEnvelope)
                      }
                      .map { _ =>
                        offsetStore.addInflight(loadedEnvelope)
                        Done
                      }
                  }
                }
              case Duplicate =>
                FutureDone
              case RejectedSeqNr =>
                replay(envelope).map(_ => Done)(ExecutionContext.parasitic)
              case RejectedBacktrackingSeqNr =>
                replay(envelope).map {
                  case true  => Done
                  case false => throwRejectedEnvelope(sourceProvider, envelope)
                }
            }
        }

        private def replay(originalEnvelope: Envelope): Future[Boolean] = {
          replayIfPossible(offsetStore, sourceProvider, originalEnvelope) { envelope =>
            if (isFilteredEvent(envelope)) {
              offsetStore.addInflight(envelope)
              FutureDone
            } else {
              r2dbcExecutor
                .withConnection("at-least-once handler") { conn =>
                  // run users handler
                  val session = new R2dbcSession(conn)
                  delegate.process(session, envelope.asInstanceOf[Envelope])
                }
                .map { _ =>
                  offsetStore.addInflight(envelope)
                  Done
                }
            }
          }
        }
      }

  }

  private[projection] def adaptedHandlerForAtLeastOnceAsync[Offset, Envelope](
      sourceProvider: SourceProvider[Offset, Envelope],
      handlerFactory: () => Handler[Envelope],
      offsetStore: R2dbcOffsetStore)(implicit ec: ExecutionContext, system: ActorSystem[_]): () => Handler[Envelope] = {
    () =>
      new AdaptedHandler(handlerFactory()) {
        override def process(envelope: Envelope): Future[Done] = {
          import R2dbcOffsetStore.Validation._
          offsetStore
            .validate(envelope)
            .flatMap {
              case Accepted =>
                if (isFilteredEvent(envelope)) {
                  offsetStore.addInflight(envelope)
                  FutureDone
                } else {
                  loadEnvelope(envelope, sourceProvider).flatMap { loadedEnvelope =>
                    delegate
                      .process(loadedEnvelope)
                      .map { _ =>
                        offsetStore.addInflight(loadedEnvelope)
                        Done
                      }
                  }
                }
              case Duplicate =>
                FutureDone
              case RejectedSeqNr =>
                replay(envelope).map(_ => Done)(ExecutionContext.parasitic)
              case RejectedBacktrackingSeqNr =>
                replay(envelope).map {
                  case true  => Done
                  case false => throwRejectedEnvelope(sourceProvider, envelope)
                }
            }
        }

        private def replay(originalEnvelope: Envelope): Future[Boolean] = {
          replayIfPossible(offsetStore, sourceProvider, originalEnvelope) { envelope =>
            if (isFilteredEvent(envelope)) {
              offsetStore.addInflight(envelope)
              FutureDone
            } else {
              delegate
                .process(envelope.asInstanceOf[Envelope])
                .map { _ =>
                  offsetStore.addInflight(envelope)
                  Done
                }
            }
          }
        }
      }
  }

  private[projection] def adaptedHandlerForGroupedAsync[Offset, Envelope](
      sourceProvider: SourceProvider[Offset, Envelope],
      handlerFactory: () => Handler[immutable.Seq[Envelope]],
      offsetStore: R2dbcOffsetStore)(
      implicit
      ec: ExecutionContext,
      system: ActorSystem[_]): () => Handler[immutable.Seq[Envelope]] = { () =>

    new AdaptedHandler(handlerFactory()) {
      override def process(envelopes: Seq[Envelope]): Future[Done] = {
        import R2dbcOffsetStore.Validation._
        offsetStore.validateAll(envelopes).flatMap { isAcceptedEnvelopes =>
          // For simplicity we process the replayed envelopes one by one (group of 1), and also store the
          // offset for each separately.
          // Important to replay them sequentially to avoid concurrent processing and offset storage.
          val replayDone =
            isAcceptedEnvelopes.foldLeft(FutureDone) {
              case (previous, (env, RejectedSeqNr)) =>
                previous.flatMap { _ =>
                  replay(env).map(_ => Done)(ExecutionContext.parasitic)
                }
              case (previous, (env, RejectedBacktrackingSeqNr)) =>
                previous.flatMap { _ =>
                  replay(env).map {
                    case true  => Done
                    case false => throwRejectedEnvelope(sourceProvider, env)
                  }
                }
              case (previous, _) =>
                previous
            }

          replayDone.flatMap { _ =>

            def processAcceptedEnvelopes(envelopes: Seq[Envelope]): Future[Done] = {
              if (envelopes.isEmpty) {
                FutureDone
              } else {
                Future.sequence(envelopes.map(env => loadEnvelope(env, sourceProvider))).flatMap { loadedEnvelopes =>
                  val offsets = loadedEnvelopes.iterator.map(extractOffsetPidSeqNr(sourceProvider, _)).toVector
                  val filteredEnvelopes = loadedEnvelopes.filterNot(isFilteredEvent)
                  if (filteredEnvelopes.isEmpty) {
                    offsetStore.saveOffsets(offsets)
                  } else {
                    delegate.process(filteredEnvelopes).flatMap { _ =>
                      offsetStore.saveOffsets(offsets)
                    }
                  }
                }
              }
            }

            val acceptedEnvelopes = isAcceptedEnvelopes.collect {
              case (env, Accepted) =>
                env
            }
            val hasRejected =
              isAcceptedEnvelopes.exists {
                case (_, RejectedSeqNr)             => true
                case (_, RejectedBacktrackingSeqNr) => true
                case _                              => false
              }

            if (hasRejected) {
              // need second validation after replay to remove duplicates
              offsetStore
                .validateAll(acceptedEnvelopes)
                .flatMap { isAcceptedEnvelopes2 =>
                  val acceptedEnvelopes2 = isAcceptedEnvelopes2.collect {
                    case (env, Accepted) => env
                  }
                  processAcceptedEnvelopes(acceptedEnvelopes2)
                }
            } else {
              processAcceptedEnvelopes(acceptedEnvelopes)
            }

          }
        }
      }

      private def replay(originalEnvelope: Envelope): Future[Boolean] = {
        replayIfPossible(offsetStore, sourceProvider, originalEnvelope) { envelope =>
          val offset = extractOffsetPidSeqNr(sourceProvider, envelope.asInstanceOf[Envelope])
          if (isFilteredEvent(envelope)) {
            offsetStore.saveOffset(offset)
          } else {
            delegate
              .process(Seq(envelope.asInstanceOf[Envelope]))
              .flatMap { _ =>
                offsetStore.saveOffset(offset)
              }
          }
        }
      }

    }
  }

  private[projection] def adaptedHandlerForFlow[Offset, Envelope](
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: FlowWithContext[Envelope, ProjectionContext, Done, ProjectionContext, _],
      offsetStore: R2dbcOffsetStore,
      settings: R2dbcProjectionSettings)(
      implicit system: ActorSystem[_]): FlowWithContext[Envelope, ProjectionContext, Done, ProjectionContext, _] = {
    import R2dbcOffsetStore.Validation._
    implicit val ec: ExecutionContext = system.executionContext

    // This is similar to DynamoDBProjectionImpl.replayIfPossible but difficult to extract common parts
    // since this is flow processing
    def replayIfPossible(originalEnvelope: Envelope): Future[Boolean] = {
      val logPrefix = offsetStore.logPrefix
      originalEnvelope match {
        case originalEventEnvelope: EventEnvelope[Any @unchecked] if originalEventEnvelope.sequenceNr > 1 =>
          val underlyingProvider = sourceProvider match {
            case adapted: JavaToScalaBySliceSourceProviderAdapter[_, _] => adapted.delegate
            case provider                                               => provider
          }
          underlyingProvider match {
            case provider: LoadEventsByPersistenceIdSourceProvider[Any @unchecked]
                if offsetStore.settings.replayOnRejectedSequenceNumbers =>
              val persistenceId = originalEventEnvelope.persistenceId
              offsetStore.storedSeqNr(persistenceId).flatMap { storedSeqNr =>
                val fromSeqNr = storedSeqNr + 1
                val toSeqNr = originalEventEnvelope.sequenceNr
                logReplayRejected(logPrefix, persistenceId, fromSeqNr, toSeqNr)
                provider.currentEventsByPersistenceId(persistenceId, fromSeqNr, toSeqNr) match {
                  case Some(querySource) =>
                    querySource
                      .mapAsync(1) { envelope =>
                        import R2dbcOffsetStore.Validation._
                        offsetStore
                          .validate(envelope)
                          .map {
                            case Accepted =>
                              if (isFilteredEvent(envelope) && settings.warnAboutFilteredEventsInFlow) {
                                log.info(
                                  "atLeastOnceFlow doesn't support skipping envelopes. Envelope [{}] still emitted.",
                                  envelope)
                              }
                              offsetStore.addInflight(envelope)
                              Some(envelope.asInstanceOf[Envelope])
                            case Duplicate =>
                              None
                            case RejectedSeqNr =>
                              // this shouldn't happen
                              throw new RejectedEnvelope(
                                s"Replay due to rejected envelope was rejected. PersistenceId [$persistenceId] seqNr [${envelope.sequenceNr}].")
                            case RejectedBacktrackingSeqNr =>
                              // this shouldn't happen
                              throw new RejectedEnvelope(
                                s"Replay due to rejected envelope was rejected. Should not be from backtracking. PersistenceId [$persistenceId] seqNr [${envelope.sequenceNr}].")
                          }
                      }
                      .collect {
                        case Some(env) =>
                          // FIXME: should we supply a projection context?
                          // FIXME: add projection telemetry to all replays? (with a new envelope source?)
                          env -> ProjectionContextImpl(sourceProvider.extractOffset(env), env, null)
                      }
                      .via(handler.asFlow)
                      .runFold(0) { case (acc, _) => acc + 1 }
                      .map { count =>
                        val expected = toSeqNr - fromSeqNr + 1
                        if (count == expected) {
                          true
                        } else {
                          // FIXME: filtered envelopes are not passed through, so we can't expect all to be replayed here
                          //        and handler could also filter out envelopes
                          log.debug(
                            "{} Replay due to rejected envelope found [{}] events, but expected [{}]. PersistenceId [{}] from seqNr [{}] to [{}].",
                            logPrefix,
                            count,
                            expected,
                            persistenceId,
                            fromSeqNr,
                            toSeqNr)
                          true
                        }
                      }
                      .recoverWith { exc =>
                        logReplayException(logPrefix, persistenceId, fromSeqNr, toSeqNr, exc)
                        Future.failed(exc)
                      }
                  case None => FutureFalse
                }
              }

            case _ =>
              triggerReplayIfPossible(sourceProvider, offsetStore, originalEnvelope)
          }
        case _ =>
          FutureFalse // no replay support for non typed envelopes
      }
    }

    FlowWithContext[Envelope, ProjectionContext]
      .mapAsync(1) { env =>
        offsetStore
          .validate(env)
          .flatMap {
            case Accepted =>
              if (isFilteredEvent(env) && settings.warnAboutFilteredEventsInFlow) {
                log.info("atLeastOnceFlow doesn't support skipping envelopes. Envelope [{}] still emitted.", env)
              }
              loadEnvelope(env, sourceProvider).map { loadedEnvelope =>
                offsetStore.addInflight(loadedEnvelope)
                Some(loadedEnvelope)
              }
            case Duplicate =>
              Future.successful(None)
            case RejectedSeqNr =>
              replayIfPossible(env).map(_ => None)(ExecutionContext.parasitic)
            case RejectedBacktrackingSeqNr =>
              replayIfPossible(env).map {
                case true  => None
                case false => throwRejectedEnvelope(sourceProvider, env)
              }
          }
      }
      .collect {
        case Some(env) =>
          env
      }
      .via(handler)
  }

  private def logReplayRejected(logPrefix: String, persistenceId: String, fromSeqNr: Long, toSeqNr: Long): Unit = {
    val msg =
      "{} Replaying events after rejected sequence number. PersistenceId [{}], replaying from seqNr [{}] to [{}]. Replay count [{}]."
    val c = replayRejectedCounter.incrementAndGet()
    if (c == 1 || c % 1000 == 0)
      log.warn(msg, logPrefix, persistenceId, fromSeqNr, toSeqNr, c)
    else
      log.debug(msg, logPrefix, persistenceId, fromSeqNr, toSeqNr, c)
  }

  private def logReplayInvalidCount(
      logPrefix: String,
      persistenceId: String,
      fromSeqNr: Long,
      toSeqNr: Long,
      count: Int,
      expected: Long): Unit = {
    log.warn(
      "{} Replay due to rejected envelope found [{}] events, but expected [{}]. PersistenceId [{}] from seqNr [{}] to [{}].",
      logPrefix,
      count,
      expected,
      persistenceId,
      fromSeqNr,
      toSeqNr)
  }

  private def logReplayException(
      logPrefix: String,
      persistenceId: String,
      fromSeqNr: Long,
      toSeqNr: Long,
      exc: Throwable): Unit = {
    log.warn(
      "{} Replay due to rejected envelope failed. PersistenceId [{}] from seqNr [{}] to [{}].",
      logPrefix,
      persistenceId,
      fromSeqNr,
      toSeqNr,
      exc)
  }

  /**
   * This replay mechanism is used by GrpcReadJournal
   */
  private def triggerReplayIfPossible[Offset, Envelope](
      sourceProvider: SourceProvider[Offset, Envelope],
      offsetStore: R2dbcOffsetStore,
      envelope: Envelope)(implicit ec: ExecutionContext): Future[Boolean] = {
    envelope match {
      case env: EventEnvelope[Any @unchecked] if env.sequenceNr > 1 =>
        sourceProvider match {
          case provider: CanTriggerReplay =>
            offsetStore.storedSeqNr(env.persistenceId).map { storedSeqNr =>
              val fromSeqNr = storedSeqNr + 1
              provider.triggerReplay(env.persistenceId, fromSeqNr, env.sequenceNr)
              true
            }
          case _ =>
            FutureFalse // no replay support for other source providers
        }
      case _ =>
        FutureFalse // no replay support for non typed envelopes
    }
  }

  private def replayIfPossible[Offset, Envelope](
      offsetStore: R2dbcOffsetStore,
      sourceProvider: SourceProvider[Offset, Envelope],
      originalEnvelope: Envelope)(accepted: EventEnvelope[Any] => Future[Done])(
      implicit ec: ExecutionContext,
      system: ActorSystem[_]): Future[Boolean] = {
    val logPrefix = offsetStore.logPrefix
    originalEnvelope match {
      case originalEventEnvelope: EventEnvelope[Any @unchecked] if originalEventEnvelope.sequenceNr > 1 =>
        val underlyingProvider = sourceProvider match {
          case adapted: JavaToScalaBySliceSourceProviderAdapter[_, _] => adapted.delegate
          case provider                                               => provider
        }
        underlyingProvider match {
          case provider: LoadEventsByPersistenceIdSourceProvider[Any @unchecked]
              if offsetStore.settings.replayOnRejectedSequenceNumbers =>
            val persistenceId = originalEventEnvelope.persistenceId
            offsetStore.storedSeqNr(persistenceId).flatMap { storedSeqNr =>
              val fromSeqNr = storedSeqNr + 1
              val toSeqNr = originalEventEnvelope.sequenceNr
              logReplayRejected(logPrefix, persistenceId, fromSeqNr, toSeqNr)
              provider.currentEventsByPersistenceId(persistenceId, fromSeqNr, toSeqNr) match {
                case Some(querySource) =>
                  querySource
                    .mapAsync(1) { envelope =>
                      import R2dbcOffsetStore.Validation._
                      offsetStore
                        .validate(envelope)
                        .flatMap {
                          case Accepted =>
                            accepted(envelope)
                          case Duplicate =>
                            FutureDone
                          case RejectedSeqNr =>
                            // this shouldn't happen
                            throw new RejectedEnvelope(
                              s"Replay due to rejected envelope was rejected. PersistenceId [$persistenceId] seqNr [${envelope.sequenceNr}].")
                          case RejectedBacktrackingSeqNr =>
                            // this shouldn't happen
                            throw new RejectedEnvelope(
                              s"Replay due to rejected envelope was rejected. Should not be from backtracking. PersistenceId [$persistenceId] seqNr [${envelope.sequenceNr}].")
                        }
                    }
                    .runFold(0) { case (acc, _) => acc + 1 }
                    .map { count =>
                      val expected = toSeqNr - fromSeqNr + 1
                      if (count == expected) {
                        true
                      } else {
                        // it's expected to find all events, otherwise fail the replay attempt
                        logReplayInvalidCount(logPrefix, persistenceId, fromSeqNr, toSeqNr, count, expected)
                        throwRejectedEnvelopeAfterFailedReplay(sourceProvider, originalEnvelope)
                      }
                    }
                    .recoverWith { exc =>
                      logReplayException(logPrefix, persistenceId, fromSeqNr, toSeqNr, exc)
                      Future.failed(exc)
                    }
                case None => FutureFalse
              }
            }

          case _ =>
            triggerReplayIfPossible(sourceProvider, offsetStore, originalEnvelope)
        }
      case _ =>
        FutureFalse // no replay support for non typed envelopes
    }
  }

  private def throwRejectedEnvelope[Offset, Envelope](
      sourceProvider: SourceProvider[Offset, Envelope],
      envelope: Envelope): Nothing = {
    extractOffsetPidSeqNr(sourceProvider, envelope) match {
      case OffsetPidSeqNr(_, Some((pid, seqNr))) =>
        throw new RejectedEnvelope(
          s"Rejected envelope from backtracking, persistenceId [$pid], seqNr [$seqNr] due to unexpected sequence number.")
      case OffsetPidSeqNr(_, None) =>
        throw new RejectedEnvelope(s"Rejected envelope from backtracking.")
    }
  }

  private def throwRejectedEnvelopeAfterFailedReplay[Offset, Envelope](
      sourceProvider: SourceProvider[Offset, Envelope],
      envelope: Envelope): Nothing = {
    val source = envelopeSourceName(envelope)
    extractOffsetPidSeqNr(sourceProvider, envelope) match {
      case OffsetPidSeqNr(_, Some((pid, seqNr))) =>
        throw new RejectedEnvelope(
          s"Replay failed, after rejected envelope from $source, persistenceId [$pid], seqNr [$seqNr], due to unexpected sequence number.")
      case OffsetPidSeqNr(_, None) =>
        throw new RejectedEnvelope(
          s"Replay failed, after rejected envelope from $source, due to unexpected sequence number.")
    }
  }

  private def envelopeSourceName[Envelope](envelope: Envelope): String = {
    envelope match {
      case env: EventEnvelope[Any @unchecked] =>
        if (EnvelopeOrigin.fromQuery(env)) "query"
        else if (EnvelopeOrigin.fromPubSub(env)) "pubsub"
        else if (EnvelopeOrigin.fromBacktracking(env)) "backtracking"
        else if (EnvelopeOrigin.fromSnapshot(env)) "snapshot"
        else env.source
      case _ => "unknown"
    }
  }

  @nowarn("msg=never used")
  abstract class AdaptedR2dbcHandler[E](val delegate: R2dbcHandler[E])(
      implicit
      ec: ExecutionContext,
      system: ActorSystem[_])
      extends Handler[E] {

    override def start(): Future[Done] =
      delegate.start()

    override def stop(): Future[Done] =
      delegate.stop()
  }

  @nowarn("msg=never used")
  abstract class AdaptedHandler[E](val delegate: Handler[E])(implicit ec: ExecutionContext, system: ActorSystem[_])
      extends Handler[E] {

    override def start(): Future[Done] =
      delegate.start()

    override def stop(): Future[Done] =
      delegate.stop()
  }

}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] class R2dbcProjectionImpl[Offset, Envelope](
    val projectionId: ProjectionId,
    r2dbcSettings: R2dbcProjectionSettings,
    settingsOpt: Option[ProjectionSettings],
    sourceProvider: SourceProvider[Offset, Envelope],
    restartBackoffOpt: Option[RestartSettings],
    val offsetStrategy: OffsetStrategy,
    handlerStrategy: HandlerStrategy,
    override val statusObserver: StatusObserver[Envelope],
    offsetStore: R2dbcOffsetStore)
    extends scaladsl.ExactlyOnceProjection[Offset, Envelope]
    with javadsl.ExactlyOnceProjection[Offset, Envelope]
    with scaladsl.GroupedProjection[Offset, Envelope]
    with javadsl.GroupedProjection[Offset, Envelope]
    with scaladsl.AtLeastOnceProjection[Offset, Envelope]
    with javadsl.AtLeastOnceProjection[Offset, Envelope]
    with scaladsl.AtLeastOnceFlowProjection[Offset, Envelope]
    with javadsl.AtLeastOnceFlowProjection[Offset, Envelope]
    with SettingsImpl[R2dbcProjectionImpl[Offset, Envelope]]
    with InternalProjection {

  private def copy(
      settingsOpt: Option[ProjectionSettings] = this.settingsOpt,
      restartBackoffOpt: Option[RestartSettings] = this.restartBackoffOpt,
      offsetStrategy: OffsetStrategy = this.offsetStrategy,
      handlerStrategy: HandlerStrategy = this.handlerStrategy,
      statusObserver: StatusObserver[Envelope] = this.statusObserver): R2dbcProjectionImpl[Offset, Envelope] =
    new R2dbcProjectionImpl(
      projectionId,
      r2dbcSettings,
      settingsOpt,
      sourceProvider,
      restartBackoffOpt,
      offsetStrategy,
      handlerStrategy,
      statusObserver,
      offsetStore)

  type ReadOffset = () => Future[Option[Offset]]

  /*
   * Build the final ProjectionSettings to use, if currently set to None fallback to values in config file
   */
  private def settingsOrDefaults(implicit system: ActorSystem[_]): ProjectionSettings = {
    val settings = settingsOpt.getOrElse(ProjectionSettings(system))
    restartBackoffOpt match {
      case None    => settings
      case Some(r) => settings.copy(restartBackoff = r)
    }
  }

  override def withRestartBackoffSettings(restartBackoff: RestartSettings): R2dbcProjectionImpl[Offset, Envelope] =
    copy(restartBackoffOpt = Some(restartBackoff))

  override def withSaveOffset(
      afterEnvelopes: Int,
      afterDuration: FiniteDuration): R2dbcProjectionImpl[Offset, Envelope] =
    copy(offsetStrategy = offsetStrategy
      .asInstanceOf[AtLeastOnce]
      .copy(afterEnvelopes = Some(afterEnvelopes), orAfterDuration = Some(afterDuration)))

  override def withGroup(
      groupAfterEnvelopes: Int,
      groupAfterDuration: FiniteDuration): R2dbcProjectionImpl[Offset, Envelope] =
    copy(handlerStrategy = handlerStrategy
      .asInstanceOf[GroupedHandlerStrategy[Envelope]]
      .copy(afterEnvelopes = Some(groupAfterEnvelopes), orAfterDuration = Some(groupAfterDuration)))

  override def withRecoveryStrategy(
      recoveryStrategy: HandlerRecoveryStrategy): R2dbcProjectionImpl[Offset, Envelope] = {
    val newStrategy = offsetStrategy match {
      case s: ExactlyOnce           => s.copy(recoveryStrategy = Some(recoveryStrategy))
      case s: AtLeastOnce           => s.copy(recoveryStrategy = Some(recoveryStrategy))
      case s: OffsetStoredByHandler => s.copy(recoveryStrategy = Some(recoveryStrategy))
      //NOTE: AtMostOnce has its own withRecoveryStrategy variant
      // this method is not available for AtMostOnceProjection
      case s: AtMostOnce => s
    }
    copy(offsetStrategy = newStrategy)
  }

  override def withStatusObserver(observer: StatusObserver[Envelope]): R2dbcProjectionImpl[Offset, Envelope] =
    copy(statusObserver = observer)

  private[projection] def actorHandlerInit[T]: Option[ActorHandlerInit[T]] =
    handlerStrategy.actorHandlerInit

  /**
   * INTERNAL API Return a RunningProjection
   */
  override private[projection] def run()(implicit system: ActorSystem[_]): RunningProjection =
    new R2dbcInternalProjectionState(settingsOrDefaults).newRunningInstance()

  /**
   * INTERNAL API
   *
   * This method returns the projection Source mapped with user 'handler' function, but before any sink attached. This
   * is mainly intended to be used by the TestKit allowing it to attach a TestSink to it.
   */
  override private[projection] def mappedSource()(implicit system: ActorSystem[_]): Source[Done, Future[Done]] =
    new R2dbcInternalProjectionState(settingsOrDefaults).mappedSource()

  private class R2dbcInternalProjectionState(settings: ProjectionSettings)(implicit val system: ActorSystem[_])
      extends InternalProjectionState[Offset, Envelope](
        projectionId,
        sourceProvider,
        offsetStrategy,
        handlerStrategy,
        statusObserver,
        settings) {

    implicit val executionContext: ExecutionContext = system.executionContext
    override val logger: LoggingAdapter = Logging(system.classicSystem, classOf[R2dbcProjectionImpl[_, _]])

    private val isExactlyOnceWithSkip: Boolean =
      offsetStrategy match {
        case ExactlyOnce(Some(Skip)) | ExactlyOnce(Some(_: RetryAndSkip)) => true
        case _                                                            => false
      }

    override def readPaused(): Future[Boolean] =
      offsetStore.readManagementState().map(_.exists(_.paused))

    override def readOffsets(): Future[Option[Offset]] =
      offsetStore.readOffset()

    // Called from InternalProjectionState.saveOffsetAndReport
    override def saveOffset(projectionId: ProjectionId, offset: Offset): Future[Done] = {
      // need the envelope to be able to call offsetStore.saveOffset
      // FIXME maybe we can cleanup this mess when moving R2dbcProjection to the Akka Projections repository? This is all internal api.
      throw new IllegalStateException(
        "Unexpected call to saveOffset. It should have called saveOffsetAndReport. Please report bug at https://github.com/akka/akka-projection/issues")
    }

    override protected def saveOffsetAndReport(
        projectionId: ProjectionId,
        projectionContext: ProjectionContextImpl[Offset, Envelope],
        batchSize: Int): Future[Done] = {
      import R2dbcProjectionImpl.FutureDone
      val envelope = projectionContext.envelope

      if (offsetStore.isInflight(envelope) || isExactlyOnceWithSkip) {
        val offset = extractOffsetPidSeqNr(projectionContext.offset, envelope)
        offsetStore
          .saveOffset(offset)
          .map { done =>
            try {
              statusObserver.offsetProgress(projectionId, envelope)
            } catch {
              case NonFatal(_) => // ignore
            }
            getTelemetry().onOffsetStored(batchSize)
            done
          }

      } else {
        FutureDone
      }
    }

    override protected def saveOffsetsAndReport(
        projectionId: ProjectionId,
        batch: immutable.Seq[ProjectionContextImpl[Offset, Envelope]]): Future[Done] = {
      import R2dbcProjectionImpl.FutureDone

      val acceptedContexts =
        if (isExactlyOnceWithSkip)
          batch.toVector
        else {
          batch.iterator.filter { ctx =>
            val env = ctx.envelope
            offsetStore.isInflight(env)
          }.toVector
        }

      if (acceptedContexts.isEmpty) {
        FutureDone
      } else {
        val offsets = acceptedContexts.map(ctx => extractOffsetPidSeqNr(ctx.offset, ctx.envelope))
        offsetStore
          .saveOffsets(offsets)
          .map { done =>
            val batchSize = acceptedContexts.map { _.groupSize }.sum
            val last = acceptedContexts.last
            try {
              statusObserver.offsetProgress(projectionId, last.envelope)
            } catch {
              case NonFatal(_) => // ignore
            }
            getTelemetry().onOffsetStored(batchSize)
            done
          }
      }
    }

    private[projection] def newRunningInstance(): RunningProjection =
      new R2dbcRunningProjection(RunningProjection.withBackoff(() => this.mappedSource(), settings), this)
  }

  private class R2dbcRunningProjection(source: Source[Done, _], projectionState: R2dbcInternalProjectionState)(
      implicit
      system: ActorSystem[_])
      extends RunningProjection
      with RunningProjectionManagement[Offset] {

    private val streamDone = source.run()

    override def stop(): Future[Done] = {
      projectionState.killSwitch.shutdown()
      // if the handler is retrying it will be aborted by this,
      // otherwise the stream would not be completed by the killSwitch until after all retries
      projectionState.abort.failure(AbortProjectionException)
      streamDone
    }

    // RunningProjectionManagement
    override def getOffset(): Future[Option[Offset]] = {
      offsetStore.getOffset()
    }

    // RunningProjectionManagement
    override def setOffset(offset: Option[Offset]): Future[Done] = {
      offset match {
        case Some(o) => offsetStore.managementSetOffset(o)
        case None    => offsetStore.managementClearOffset()
      }
    }

    // RunningProjectionManagement
    override def getManagementState(): Future[Option[ManagementState]] =
      offsetStore.readManagementState()

    // RunningProjectionManagement
    override def setPaused(paused: Boolean): Future[Done] =
      offsetStore.savePaused(paused)
  }

}
