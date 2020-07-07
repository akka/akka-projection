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
import akka.projection.RunningProjection
import akka.projection.internal.NoopStatusObserver
import akka.projection.internal.SettingsImpl
import akka.projection.internal.RestartBackoffSettings
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.SourceProvider

// FIXME: this should be refactored as part of #198
object TestProjection {
  def apply[Offset, Envelope](
      system: ActorSystem[_],
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: Handler[Envelope]): Projection[Envelope] =
    new TestProjection(projectionId, sourceProvider, handler)(system)
}

class TestProjection[Offset, Envelope](
    val projectionId: ProjectionId,
    sourceProvider: SourceProvider[Offset, Envelope],
    handler: Handler[Envelope])(implicit val system: ActorSystem[_])
    extends Projection[Envelope]
    with SettingsImpl[TestProjection[Offset, Envelope]] {

  override val statusObserver: StatusObserver[Envelope] = NoopStatusObserver

  override def withStatusObserver(observer: StatusObserver[Envelope]): TestProjection[Offset, Envelope] =
    this // no need for StatusObserver in tests

  final override def withRestartBackoffSettings(
      restartBackoff: RestartBackoffSettings): TestProjection[Offset, Envelope] = this
  override def withSaveOffset(afterEnvelopes: Int, afterDuration: FiniteDuration): TestProjection[Offset, Envelope] =
    this
  override def withGroup(
      groupAfterEnvelopes: Int,
      groupAfterDuration: FiniteDuration): TestProjection[Offset, Envelope] = this

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
  private class InternalProjectionState(sourceProvider: SourceProvider[Offset, Envelope], handler: Handler[Envelope])(
      implicit val system: ActorSystem[_]) {

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
  def apply[Offset, Envelope](
      sourceEvents: List[Envelope],
      extractOffset: Envelope => Offset,
      extractCreationTime: Envelope => Long = (_: Envelope) => 0L): SourceProvider[Offset, Envelope] = {
    new TestSourceProvider[Offset, Envelope](sourceEvents, extractOffset, extractCreationTime)
  }
}

class TestSourceProvider[Offset, Envelope] private[projection] (
    sourceEvents: List[Envelope],
    _extractOffset: Envelope => Offset,
    _extractCreationTime: Envelope => Long)
    extends SourceProvider[Offset, Envelope] {
  override def source(offset: () => Future[Option[Offset]]): Future[Source[Envelope, NotUsed]] =
    Future.successful {
      Source(sourceEvents).concat(Source.maybe)
    }

  override def extractOffset(envelope: Envelope): Offset = _extractOffset(envelope)

  override def extractCreationTime(envelope: Envelope): Long = _extractCreationTime(envelope)
}

class TestEnvelope[Event] {}
