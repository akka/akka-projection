/*
 * Copyright (C) 2020-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.eventsourced.scaladsl

import java.time.Instant

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.actor.typed.ActorRef
import akka.annotation.InternalApi
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.scaladsl.EventsByTagQuery
import akka.persistence.query.scaladsl.ReadJournal
import akka.persistence.query.typed.scaladsl.CurrentEventsByPersistenceIdTypedQuery
import akka.persistence.query.typed.scaladsl.EventTimestampQuery
import akka.persistence.query.typed.scaladsl.EventsBySliceQuery
import akka.persistence.query.typed.scaladsl.EventsBySliceStartingFromSnapshotsQuery
import akka.persistence.query.typed.scaladsl.LoadEventQuery
import akka.projection.BySlicesSourceProvider
import akka.projection.eventsourced.EventEnvelope
import akka.projection.internal.CanTriggerReplay
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source
import akka.stream.BoundedSourceQueue
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.TimerScheduler
import scala.annotation.tailrec
import akka.stream.QueueOfferResult
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicReference
import java.util.UUID
import akka.actor.typed.Props

object EventSourcedProvider {

  def eventsByTag[Event](
      system: ActorSystem[_],
      readJournalPluginId: String,
      tag: String): SourceProvider[Offset, EventEnvelope[Event]] = {
    val eventsByTagQuery =
      PersistenceQuery(system).readJournalFor[EventsByTagQuery](readJournalPluginId)
    eventsByTag(system, eventsByTagQuery, tag)
  }

  def eventsByTag[Event](
      system: ActorSystem[_],
      eventsByTagQuery: EventsByTagQuery,
      tag: String): SourceProvider[Offset, EventEnvelope[Event]] = {
    new EventsByTagSourceProvider(system, eventsByTagQuery, tag)
  }

  private class EventsByTagSourceProvider[Event](
      system: ActorSystem[_],
      eventsByTagQuery: EventsByTagQuery,
      tag: String)
      extends SourceProvider[Offset, EventEnvelope[Event]] {
    implicit val executionContext: ExecutionContext = system.executionContext

    override def source(offset: () => Future[Option[Offset]]): Future[Source[EventEnvelope[Event], NotUsed]] =
      offset().map { offsetOpt =>
        val offset = offsetOpt.getOrElse(NoOffset)
        eventsByTagQuery
          .eventsByTag(tag, offset)
          .map(env => EventEnvelope(env))
      }

    override def extractOffset(envelope: EventEnvelope[Event]): Offset = envelope.offset

    override def extractCreationTime(envelope: EventEnvelope[Event]): Long = envelope.timestamp
  }

  def eventsBySlices[Event](
      system: ActorSystem[_],
      readJournalPluginId: String,
      entityType: String,
      minSlice: Int,
      maxSlice: Int): SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]] = {
    val eventsBySlicesQuery =
      PersistenceQuery(system).readJournalFor[EventsBySliceQuery](readJournalPluginId)
    eventsBySlices(system, eventsBySlicesQuery, entityType, minSlice, maxSlice)
  }

  /**
   * By default, the `SourceProvider` uses the stored offset when starting the Projection. This offset can be adjusted
   * by defining the `adjustStartOffset` function, which is a function from loaded offset (if any) to the
   * adjusted offset that will be used to by the `eventsBySlicesQuery`.
   */
  def eventsBySlices[Event](
      system: ActorSystem[_],
      readJournalPluginId: String,
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      adjustStartOffset: Option[Offset] => Future[Option[Offset]])
      : SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]] = {
    val eventsBySlicesQuery =
      PersistenceQuery(system).readJournalFor[EventsBySliceQuery](readJournalPluginId)
    eventsBySlices(system, eventsBySlicesQuery, entityType, minSlice, maxSlice, adjustStartOffset)
  }

  def eventsBySlices[Event](
      system: ActorSystem[_],
      eventsBySlicesQuery: EventsBySliceQuery,
      entityType: String,
      minSlice: Int,
      maxSlice: Int): SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]] = {
    eventsBySlices(system, eventsBySlicesQuery, entityType, minSlice, maxSlice, offset => Future.successful(offset))
  }

  /**
   * By default, the `SourceProvider` uses the stored offset when starting the Projection. This offset can be adjusted
   * by defining the `adjustStartOffset` function, which is a function from loaded offset (if any) to the
   * adjusted offset that will be used to by the `eventsBySlicesQuery`.
   */
  def eventsBySlices[Event](
      system: ActorSystem[_],
      eventsBySlicesQuery: EventsBySliceQuery,
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      adjustStartOffset: Option[Offset] => Future[Option[Offset]])
      : SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]] = {
    eventsBySlicesQuery match {
      case query: EventsBySliceQuery with CanTriggerReplay =>
        new EventsBySlicesSourceProvider[Event](
          system,
          eventsBySlicesQuery,
          entityType,
          minSlice,
          maxSlice,
          adjustStartOffset) with CanTriggerReplay {
          override private[akka] def triggerReplay(
              persistenceId: String,
              fromSeqNr: Long,
              triggeredBySeqNr: Long): Unit =
            query.triggerReplay(persistenceId, fromSeqNr, triggeredBySeqNr)
        }

      // TODO make this opt-out/opt-in
      case query: EventsBySliceQuery with CurrentEventsByPersistenceIdTypedQuery =>
        new ReplayableEventsBySlicesSourceProvider[Event](
          system,
          query,
          entityType,
          minSlice,
          maxSlice,
          adjustStartOffset)

      case _ =>
        new EventsBySlicesSourceProvider(system, eventsBySlicesQuery, entityType, minSlice, maxSlice, adjustStartOffset)
    }
  }

  def eventsBySlicesStartingFromSnapshots[Snapshot, Event](
      system: ActorSystem[_],
      readJournalPluginId: String,
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      transformSnapshot: Snapshot => Event)
      : SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]] = {
    val eventsBySlicesQuery =
      PersistenceQuery(system).readJournalFor[EventsBySliceStartingFromSnapshotsQuery](readJournalPluginId)
    eventsBySlicesStartingFromSnapshots(system, eventsBySlicesQuery, entityType, minSlice, maxSlice, transformSnapshot)
  }

  /**
   * By default, the `SourceProvider` uses the stored offset when starting the Projection. This offset can be adjusted
   * by defining the `adjustStartOffset` function, which is a function from loaded offset (if any) to the
   * adjusted offset that will be used to by the `eventsBySlicesQuery`.
   */
  def eventsBySlicesStartingFromSnapshots[Snapshot, Event](
      system: ActorSystem[_],
      readJournalPluginId: String,
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      transformSnapshot: Snapshot => Event,
      adjustStartOffset: Option[Offset] => Future[Option[Offset]])
      : SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]] = {
    val eventsBySlicesQuery =
      PersistenceQuery(system).readJournalFor[EventsBySliceStartingFromSnapshotsQuery](readJournalPluginId)
    eventsBySlicesStartingFromSnapshots(
      system,
      eventsBySlicesQuery,
      entityType,
      minSlice,
      maxSlice,
      transformSnapshot,
      adjustStartOffset)
  }

  def eventsBySlicesStartingFromSnapshots[Snapshot, Event](
      system: ActorSystem[_],
      eventsBySlicesQuery: EventsBySliceStartingFromSnapshotsQuery,
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      transformSnapshot: Snapshot => Event): SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]] =
    eventsBySlicesStartingFromSnapshots(
      system,
      eventsBySlicesQuery,
      entityType,
      minSlice,
      maxSlice,
      transformSnapshot,
      offset => Future.successful(offset))

  /**
   * By default, the `SourceProvider` uses the stored offset when starting the Projection. This offset can be adjusted
   * by defining the `adjustStartOffset` function, which is a function from loaded offset (if any) to the
   * adjusted offset that will be used to by the `eventsBySlicesQuery`.
   */
  def eventsBySlicesStartingFromSnapshots[Snapshot, Event](
      system: ActorSystem[_],
      eventsBySlicesQuery: EventsBySliceStartingFromSnapshotsQuery,
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      transformSnapshot: Snapshot => Event,
      adjustStartOffset: Option[Offset] => Future[Option[Offset]])
      : SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]] = {
    eventsBySlicesQuery match {
      case query: EventsBySliceStartingFromSnapshotsQuery with CanTriggerReplay =>
        new EventsBySlicesStartingFromSnapshotsSourceProvider[Snapshot, Event](
          system,
          eventsBySlicesQuery,
          entityType,
          minSlice,
          maxSlice,
          transformSnapshot,
          adjustStartOffset) with CanTriggerReplay {
          override private[akka] def triggerReplay(
              persistenceId: String,
              fromSeqNr: Long,
              triggeredBySeqNr: Long): Unit =
            query.triggerReplay(persistenceId, fromSeqNr, triggeredBySeqNr)
        }

      // TODO: make this opt-out/opt-in
      case query: EventsBySliceStartingFromSnapshotsQuery with CurrentEventsByPersistenceIdTypedQuery =>
        new ReplayableEventsBySlicesStartingFromSnapshotsSourceProvider[Snapshot, Event](
          system,
          query,
          entityType,
          minSlice,
          maxSlice,
          transformSnapshot,
          adjustStartOffset)

      case _ =>
        new EventsBySlicesStartingFromSnapshotsSourceProvider(
          system,
          eventsBySlicesQuery,
          entityType,
          minSlice,
          maxSlice,
          transformSnapshot,
          adjustStartOffset)
    }
  }

  def sliceForPersistenceId(system: ActorSystem[_], readJournalPluginId: String, persistenceId: String): Int =
    PersistenceQuery(system)
      .readJournalFor[EventsBySliceQuery](readJournalPluginId)
      .sliceForPersistenceId(persistenceId)

  def sliceRanges(system: ActorSystem[_], readJournalPluginId: String, numberOfRanges: Int): immutable.Seq[Range] =
    PersistenceQuery(system).readJournalFor[EventsBySliceQuery](readJournalPluginId).sliceRanges(numberOfRanges)

  private class EventsBySlicesSourceProvider[Event](
      system: ActorSystem[_],
      eventsBySlicesQuery: EventsBySliceQuery,
      entityType: String,
      override val minSlice: Int,
      override val maxSlice: Int,
      adjustStartOffset: Option[Offset] => Future[Option[Offset]])
      extends SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]]
      with BySlicesSourceProvider
      with EventTimestampQuerySourceProvider
      with LoadEventQuerySourceProvider {
    implicit val executionContext: ExecutionContext = system.executionContext

    override def readJournal: ReadJournal = eventsBySlicesQuery

    override def source(offset: () => Future[Option[Offset]])
        : Future[Source[akka.persistence.query.typed.EventEnvelope[Event], NotUsed]] = {
      for {
        storedOffset <- offset()
        startOffset <- adjustStartOffset(storedOffset)
      } yield {
        eventsBySlicesQuery.eventsBySlices(entityType, minSlice, maxSlice, startOffset.getOrElse(NoOffset))
      }
    }

    override def extractOffset(envelope: akka.persistence.query.typed.EventEnvelope[Event]): Offset = envelope.offset

    override def extractCreationTime(envelope: akka.persistence.query.typed.EventEnvelope[Event]): Long =
      envelope.timestamp

  }

  private class ReplayableEventsBySlicesSourceProvider[Event](
      override val system: ActorSystem[_],
      query: EventsBySliceQuery with CurrentEventsByPersistenceIdTypedQuery,
      entityType: String,
      override val minSlice: Int,
      override val maxSlice: Int,
      adjustStartOffset: Option[Offset] => Future[Option[Offset]])
      extends EventsBySlicesSourceProvider[Event](system, query, entityType, minSlice, maxSlice, adjustStartOffset)
      with CanTriggerReplay
      with HasGapFillerActor {

    /** INTERNAL API */
    @InternalApi
    override private[scaladsl] val gapFillerRef = new AtomicReference()

    override val entityTypeSliceRange = s"$entityType-$minSlice-$maxSlice"
    override def byPersistenceIdQuery: CurrentEventsByPersistenceIdTypedQuery = query

    override private[akka] def triggerReplay(persistenceId: String, fromSeqNr: Long, triggeredBySeqNr: Long): Unit =
      fill(persistenceId, fromSeqNr, triggeredBySeqNr)

    override def source(offset: () => Future[Option[Offset]])
        : Future[Source[akka.persistence.query.typed.EventEnvelope[Event], NotUsed]] = {
      val upstreamFut = super.source(offset)

      val requestedReplays =
        Source
          .queue[Source[akka.persistence.query.typed.EventEnvelope[Event], NotUsed]](1)
          // TODO: is there some way to fail if the query didn't give us the expected number of events?
          .flatMapConcat(identity)
          .mapMaterializedValue { queue =>
            registerStream(queue)
            NotUsed
          }

      upstreamFut.map { upstream =>
        // prefer the requested replay elements
        requestedReplays.mergePreferred(upstream, false, eagerComplete = true)
      }
    }
  }

  /** INTERNAL API */
  @InternalApi
  private[scaladsl] trait HasGapFillerActor {
    /* 1. initialized to null by extending class
     * 2. on access (most likely in registerStream):
     *    - finds null
     *    - spawns gapfiller actor
     *    - tries to set
     *    - if loses, stops the spawned gapfiller actor
     * 3. gapFillerRef was observed to hold the winner
     * 4. after a period with no registered streams, the gap filler actor
     *    begins the process of stopping by setting this AR to null (effectively back to 1); the
     *    filler actor will forward its external API messages to
     *    a successor via this trait
     */
    private[scaladsl] def gapFillerRef: AtomicReference[ActorRef[GapFillerActor.Command]]
    def system: ActorSystem[_]
    def entityTypeSliceRange: String
    def byPersistenceIdQuery: CurrentEventsByPersistenceIdTypedQuery

    @tailrec
    final def registerStream[Event](
        queue: BoundedSourceQueue[Source[akka.persistence.query.typed.EventEnvelope[Event], NotUsed]]): Unit = {
      val filler = gapFillerRef.get()
      if (filler == null) {
        // No gap filler actor, so spawn one
        spawnGapFiller()
        registerStream(queue)
      } else {
        filler ! GapFillerActor.RegisterStreamInjection(queue)
      }
    }

    @tailrec
    final def fill(persistenceId: String, fromSeqNr: Long, triggeredBySeqNr: Long): Unit = {
      val filler = gapFillerRef.get()
      if (filler == null) {
        spawnGapFiller()
        fill(persistenceId, fromSeqNr, triggeredBySeqNr)
      } else {
        filler ! GapFillerActor.Fill(persistenceId, fromSeqNr, triggeredBySeqNr)
        if (!gapFillerRef.compareAndSet(filler, filler)) {
          fill(persistenceId, fromSeqNr, triggeredBySeqNr)
        }
      }
    }

    private def spawnGapFiller(): Unit = {
      val gapFillerName = s"gap-filler-${entityTypeSliceRange}-${UUID.randomUUID()}"
      val gapFiller = system.systemActorOf(GapFillerActor(this), gapFillerName, Props.empty)
      if (!gapFillerRef.compareAndSet(null, gapFiller)) {
        // we lost the race, stop the one we just started and use the winner
        gapFiller ! GapFillerActor.StopGapFiller(false)
      }
    }
  }

  object GapFillerActor {
    sealed trait Command

    case class RegisterStreamInjection[Event](
        queue: BoundedSourceQueue[Source[akka.persistence.query.typed.EventEnvelope[Event], NotUsed]])
        extends Command
    case class Fill(persistenceId: String, fromSeqNr: Long, triggeredBySeqNr: Long) extends Command
    case object TryNext extends Command
    case class StopGapFiller(first: Boolean) extends Command

    def apply[Event](provider: HasGapFillerActor): Behavior[Command] =
      Behaviors.withTimers { timers =>
        timers.startSingleTimer(classOf[StopGapFiller], StopGapFiller(true), 10.minutes)
        apply(State(provider.byPersistenceIdQuery, None, Nil, Nil, Map.empty, 1.milli), timers, provider)
      }

    case class State[Event](
        readJournal: CurrentEventsByPersistenceIdTypedQuery,
        currentQuery: Option[Source[akka.persistence.query.typed.EventEnvelope[Event], NotUsed]],
        registeredQueues: List[BoundedSourceQueue[Source[akka.persistence.query.typed.EventEnvelope[Event], NotUsed]]],
        pendingQueues: List[BoundedSourceQueue[Source[akka.persistence.query.typed.EventEnvelope[Event], NotUsed]]],
        pendingGaps: Map[String, immutable.NumericRange.Inclusive[Long]],
        backoff: FiniteDuration) {
      def registerQueue(queue: BoundedSourceQueue[_]): State[Event] =
        copy(registeredQueues = queue.asInstanceOf[BoundedSourceQueue[
            Source[akka.persistence.query.typed.EventEnvelope[Event], NotUsed]]] :: registeredQueues)

      def withPendingGap(persistenceId: String, fromSeqNr: Long, toSeqNr: Long): State[Event] = {
        val newRange = Range.Long.inclusive(fromSeqNr, toSeqNr, 1)
        val newPendingGaps = pendingGaps.updatedWith(persistenceId) {
          _ match {
            case entry @ Some(existingRange) =>
              if (newRange == existingRange) entry
              else
                Some(Range.Long.inclusive(existingRange.min.min(newRange.min), existingRange.max.max(newRange.max), 1))

            case None => Some(newRange)
          }
        }
        copy(pendingGaps = newPendingGaps)
      }
    }

    @tailrec
    def tryOrDefer[Event](state: State[Event], timers: TimerScheduler[Command]): State[Event] =
      if (state.registeredQueues.nonEmpty) {
        if (state.currentQuery.isEmpty && state.pendingGaps.nonEmpty) {
          // Start working this gap
          val (persistenceId, range) = state.pendingGaps.head
          val query = state.readJournal.currentEventsByPersistenceIdTyped[Event](persistenceId, range.min, range.max)
          tryOrDefer(
            state.copy(
              currentQuery = Some(query),
              pendingQueues = state.registeredQueues,
              pendingGaps = state.pendingGaps.removed(persistenceId)),
            timers)
        } else if (state.currentQuery.nonEmpty) {
          if (state.pendingQueues.nonEmpty) {
            // Work as many queues as we can
            var queues = state.pendingQueues
            var registered = state.registeredQueues
            val query = state.currentQuery.get
            var continue: Boolean = true

            while (continue && queues.nonEmpty) {
              val target = queues.head
              target.offer(query) match {
                case QueueOfferResult.Enqueued =>
                  queues = queues.tail

                case QueueOfferResult.QueueClosed | QueueOfferResult.Failure(_) =>
                  // Stream being fed completed or failed
                  queues = queues.tail
                  registered = registered.filterNot(_ == target)

                case QueueOfferResult.Dropped =>
                  // Buffer is full
                  continue = false
              }
            }

            if (queues.isEmpty) {
              // recur so we can maybe try the next pending gap
              tryOrDefer(
                state.copy(currentQuery = None, registeredQueues = registered, pendingQueues = Nil, backoff = 1.milli),
                timers)
            } else {
              // defer the next step
              timers.startSingleTimer(TryNext, TryNext, state.backoff)
              val jitter = ThreadLocalRandom.current().nextDouble(0.03)
              val nextBackoff = state.backoff * (jitter + 2.0)
              state.copy(
                registeredQueues = registered,
                pendingQueues = queues,
                backoff = if (nextBackoff.isFinite) nextBackoff.toMillis.millis else state.backoff)
            }
          } else {
            // Done with this query
            tryOrDefer(state.copy(currentQuery = None), timers)
          }
        } else {
          // No current query, no pending gaps, nothing to do
          state
        }
      } else {
        // No streams registered, so ensure that this actor stops itself
        if (!timers.isTimerActive(classOf[StopGapFiller])) {
          timers.startSingleTimer(classOf[StopGapFiller], StopGapFiller(first = true), 10.minutes)
        }
        state.copy(pendingGaps = Map.empty)
      }

    private def apply[Event](
        state: State[Event],
        timers: TimerScheduler[Command],
        provider: HasGapFillerActor): Behavior[Command] =
      Behaviors.receive { (context, msg) =>
        msg match {
          case RegisterStreamInjection(queue) =>
            if (provider.gapFillerRef.get() == context.self) {
              timers.cancel(classOf[StopGapFiller])
              apply(state.registerQueue(queue), timers, provider)
            } else {
              // This actor is going to stop soon
              provider.registerStream(queue)
              Behaviors.same
            }

          case Fill(persistenceId, fromSeqNr, triggeredBySeqNr) =>
            if (provider.gapFillerRef.get() != context.self) {
              provider.fill(persistenceId, fromSeqNr, triggeredBySeqNr)
              Behaviors.same
            } else if (state.registeredQueues.nonEmpty) {
              context.log.debug(
                "Received request to replay events for [{}] from seqNr={} to seqNr={}",
                persistenceId,
                fromSeqNr,
                triggeredBySeqNr)
              val newState =
                tryOrDefer(state.withPendingGap(persistenceId, fromSeqNr, triggeredBySeqNr), timers)

              apply(newState, timers, provider)
            } else {
              context.log.info(
                "Ignoring request to replay events for [{}] from seqNr={} to seqNr={}, due to no registered streams",
                persistenceId,
                fromSeqNr,
                triggeredBySeqNr)
              Behaviors.same
            }

          case TryNext =>
            apply(tryOrDefer(state, timers), timers, provider)

          case StopGapFiller(true) =>
            // too much time since the last stream registered, prepare for termination
            provider.gapFillerRef.set(null)
            timers.startSingleTimer(classOf[StopGapFiller], StopGapFiller(false), 1.minute)
            Behaviors.same

          case StopGapFiller(false) =>
            Behaviors.stopped
        }
      }
  }

  private class EventsBySlicesStartingFromSnapshotsSourceProvider[Snapshot, Event](
      system: ActorSystem[_],
      eventsBySlicesQuery: EventsBySliceStartingFromSnapshotsQuery,
      entityType: String,
      override val minSlice: Int,
      override val maxSlice: Int,
      transformSnapshot: Snapshot => Event,
      adjustStartOffset: Option[Offset] => Future[Option[Offset]])
      extends SourceProvider[Offset, akka.persistence.query.typed.EventEnvelope[Event]]
      with BySlicesSourceProvider
      with EventTimestampQuerySourceProvider
      with LoadEventQuerySourceProvider {
    implicit val executionContext: ExecutionContext = system.executionContext

    override def readJournal: ReadJournal = eventsBySlicesQuery

    override def source(offset: () => Future[Option[Offset]])
        : Future[Source[akka.persistence.query.typed.EventEnvelope[Event], NotUsed]] = {
      for {
        storedOffset <- offset()
        startOffset <- adjustStartOffset(storedOffset)
      } yield {
        eventsBySlicesQuery.eventsBySlicesStartingFromSnapshots(
          entityType,
          minSlice,
          maxSlice,
          startOffset.getOrElse(NoOffset),
          transformSnapshot)
      }
    }

    override def extractOffset(envelope: akka.persistence.query.typed.EventEnvelope[Event]): Offset = envelope.offset

    override def extractCreationTime(envelope: akka.persistence.query.typed.EventEnvelope[Event]): Long =
      envelope.timestamp

  }

  private class ReplayableEventsBySlicesStartingFromSnapshotsSourceProvider[Snapshot, Event](
      override val system: ActorSystem[_],
      query: EventsBySliceStartingFromSnapshotsQuery with CurrentEventsByPersistenceIdTypedQuery,
      entityType: String,
      override val minSlice: Int,
      override val maxSlice: Int,
      transformSnapshot: Snapshot => Event,
      adjustStartOffset: Option[Offset] => Future[Option[Offset]])
      extends EventsBySlicesStartingFromSnapshotsSourceProvider[Snapshot, Event](
        system,
        query,
        entityType,
        minSlice,
        maxSlice,
        transformSnapshot,
        adjustStartOffset)
      with CanTriggerReplay
      with HasGapFillerActor {

    /** INTERNAL API */
    @InternalApi
    override private[scaladsl] val gapFillerRef = new AtomicReference()

    override val entityTypeSliceRange = s"$entityType-$minSlice-$maxSlice"
    override def byPersistenceIdQuery: CurrentEventsByPersistenceIdTypedQuery = query

    override private[akka] def triggerReplay(persistenceId: String, fromSeqNr: Long, triggeredBySeqNr: Long): Unit =
      fill(persistenceId, fromSeqNr, triggeredBySeqNr)

    override def source(offset: () => Future[Option[Offset]])
        : Future[Source[akka.persistence.query.typed.EventEnvelope[Event], NotUsed]] = {
      val upstreamFut = super.source(offset)

      val requestedReplays =
        Source
          .queue[Source[akka.persistence.query.typed.EventEnvelope[Event], NotUsed]](1)
          // TODO: is there some way to fail if the query didn't give us the expected number of events?
          .flatMapConcat(identity)
          .mapMaterializedValue { queue =>
            registerStream(queue)
            NotUsed
          }

      upstreamFut.map { upstream =>
        // prefer the requested replay elements
        requestedReplays.mergePreferred(upstream, false, eagerComplete = true)
      }
    }
  }

  private trait EventTimestampQuerySourceProvider extends EventTimestampQuery {
    def readJournal: ReadJournal

    override def timestampOf(persistenceId: String, sequenceNr: Long): Future[Option[Instant]] =
      readJournal match {
        case timestampQuery: EventTimestampQuery =>
          timestampQuery.timestampOf(persistenceId, sequenceNr)
        case _ =>
          Future.failed(
            new IllegalStateException(
              s"[${readJournal.getClass.getName}] must implement [${classOf[EventTimestampQuery].getName}]"))
      }
  }

  private trait LoadEventQuerySourceProvider extends LoadEventQuery {
    def readJournal: ReadJournal

    override def loadEnvelope[Evt](
        persistenceId: String,
        sequenceNr: Long): Future[akka.persistence.query.typed.EventEnvelope[Evt]] =
      readJournal match {
        case laodEventQuery: LoadEventQuery =>
          laodEventQuery.loadEnvelope(persistenceId, sequenceNr)
        case _ =>
          Future.failed(
            new IllegalStateException(
              s"[${readJournal.getClass.getName}] must implement [${classOf[LoadEventQuery].getName}]"))
      }
  }

}
