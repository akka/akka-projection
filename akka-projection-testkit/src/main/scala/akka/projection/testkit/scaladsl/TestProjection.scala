package akka.projection.testkit.scaladsl

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import akka.actor.typed.ActorSystem
import akka.projection.StatusObserver
import akka.projection.internal.ActorHandlerInit
import akka.stream.scaladsl.Source
import akka.NotUsed
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.stream.KillSwitches
import akka.stream.SharedKillSwitch
import akka.Done
import akka.persistence.query.Offset
import akka.projection.RunningProjection
import akka.projection.internal.NoopStatusObserver
import akka.projection.internal.SettingsImpl
import akka.projection.internal.RestartBackoffSettings
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.SourceProvider
import akka.projection.EventEnvelope

// FIXME: this should be refactored as part of #198
object TestProjection {
  def apply[Event](
      system: ActorSystem[_],
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, EventEnvelope[Event]],
      handler: Handler[EventEnvelope[Event]]): Projection[EventEnvelope[Event]] =
    new TestProjection(projectionId, sourceProvider, handler)(system)
}

class TestProjection[Event](
    val projectionId: ProjectionId,
    sourceProvider: SourceProvider[Offset, EventEnvelope[Event]],
    handler: Handler[EventEnvelope[Event]])(implicit val system: ActorSystem[_])
    extends Projection[EventEnvelope[Event]]
    with SettingsImpl[TestProjection[Event]] {

  override val statusObserver: StatusObserver[EventEnvelope[Event]] = NoopStatusObserver

  override def withStatusObserver(observer: StatusObserver[EventEnvelope[Event]]): TestProjection[Event] =
    this // no need for StatusObserver in tests

  final override def withRestartBackoffSettings(restartBackoff: RestartBackoffSettings): TestProjection[Event] = this
  override def withSaveOffset(afterEnvelopes: Int, afterDuration: FiniteDuration): TestProjection[Event] = this
  override def withGroup(groupAfterEnvelopes: Int, groupAfterDuration: FiniteDuration): TestProjection[Event] = this

  private[akka] def actorHandlerInit[T]: Option[ActorHandlerInit[T]] = None

  override def run()(implicit system: ActorSystem[_]): RunningProjection =
    new InternalProjectionState(sourceProvider, handler).newRunningInstance()

  private[projection] def mappedSource()(implicit system: ActorSystem[_]): Source[Done, Future[Done]] =
    new InternalProjectionState(sourceProvider, handler).mappedSource()

  /*
   * INTERNAL API
   * This internal class will hold the KillSwitch that is needed
   * when building the mappedSource and when running the projection (to stop)
   */
  private class InternalProjectionState(
      sourceProvider: SourceProvider[Offset, EventEnvelope[Event]],
      handler: Handler[EventEnvelope[Event]])(implicit val system: ActorSystem[_]) {

    private val killSwitch = KillSwitches.shared(projectionId.id)

    def mappedSource(): Source[Done, Future[Done]] =
      Source
        .futureSource(sourceProvider
          .source(null))
        .via(killSwitch.flow)
        .mapAsync(1)(i => handler.process(i))
        .mapMaterializedValue(_ => Future.successful(Done))

    def newRunningInstance(): RunningProjection =
      new TestRunningProjection(mappedSource(), killSwitch)
  }

  private class TestRunningProjection(val source: Source[Done, _], killSwitch: SharedKillSwitch)
      extends RunningProjection {

    private val futureDone = source.run()

    override def stop(): Future[Done] = {
      killSwitch.shutdown()
      futureDone
    }
  }
}

// FIXME: this should be replaced as part of #198
object TestSourceProvider {
  def apply[Event](sourceEvents: List[(Long, Event)]): SourceProvider[Offset, EventEnvelope[Event]] = {
    new TestSourceProvider[Event](sourceEvents)
  }
}

class TestSourceProvider[Event](sourceEvents: List[(Long, Event)])
    extends SourceProvider[Offset, EventEnvelope[Event]] {
  override def source(offset: () => Future[Option[Offset]]): Future[Source[EventEnvelope[Event], NotUsed]] =
    Future.successful {
      val envelopes = sourceEvents.map {
        case (offset, event) =>
          val o = Offset.sequence(offset)
          new EventEnvelope(o, "", 0L, event, 0L)
      }
      Source(envelopes).concat(Source.maybe)
    }

  override def extractOffset(envelope: EventEnvelope[Event]): Offset = envelope.offset
}
