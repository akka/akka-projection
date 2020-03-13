/*
 * Copyright (C) 2019-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl.kafka

import akka.kafka.CommitterSettings
import akka.kafka.ConsumerMessage.{ CommittableMessage, CommittableOffset }
import akka.kafka.scaladsl.Consumer.{ Control, DrainingControl }
import akka.kafka.scaladsl.{ Committer, Consumer }
import akka.projection.scaladsl._
import akka.stream.Materializer
import akka.stream.scaladsl.{ Keep, Sink, Source }
import akka.{ Done, NotUsed }
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.concurrent.{ ExecutionContext, Future }

object KafkaProjection {

  def apply[K, V](
      src: Source[ConsumerRecord[K, V], Control],
      handler: EventHandler[V, Future[Done]]): Projection[ConsumerRecord[K, V], V, NotUsed, Future[Done]] =
    new ConsumerRecordProjection(src, handler)

  def apply[K, V, R](
      sourceProvider: SourceProvider[Long, ConsumerRecord[K, V], Control],
      handler: EventHandler[V, R],
      projectionRunner: ProjectionRunner[Long, R]): Projection[ConsumerRecord[K, V], V, Long, R] = {
    new ConsumerRecordProjectionWithRunner(sourceProvider, handler, projectionRunner)
  }

  def atLeastOnce[K, V](
      committableSource: Source[CommittableMessage[K, V], Control],
      committerSettings: CommitterSettings,
      handler: EventHandler[V, Future[Done]])
      : Projection[CommittableMessage[K, V], V, CommittableOffset, Future[Done]] = {
    new AtLeastOnceKafkaProjection(committableSource, committerSettings, handler)
  }

}

class AtLeastOnceKafkaProjection[K, V](
    committableSource: Source[CommittableMessage[K, V], Control],
    committerSettings: CommitterSettings,
    handler: EventHandler[V, Future[Done]])
    extends Projection[CommittableMessage[K, V], V, CommittableOffset, Future[Done]] {

  private var control: Option[DrainingControl[Done]] = None

  override def processEnvelope(envelope: CommittableMessage[K, V])(implicit ex: ExecutionContext): Future[Done] =
    handler.onEvent(envelope.record.value())

  override def start()(implicit ex: ExecutionContext, materializer: Materializer): Unit = {

    val drainingControl =
      committableSource
        .mapAsync(1) { envelope =>
          processEnvelope(envelope).map(_ => envelope.committableOffset)
        }
        .via(Committer.flow(committerSettings))
        .toMat(Sink.ignore)(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()

    control = Some(drainingControl)
  }

  override def stop()(implicit ex: ExecutionContext): Future[Done] =
    control match {
      case Some(control) => control.drainAndShutdown().map(_ => Done)
      case _             => Future.successful(Done)
    }

}

class ConsumerRecordProjection[K, V](
    consumerRecord: Source[ConsumerRecord[K, V], Control],
    handler: EventHandler[V, Future[Done]])
    extends Projection[ConsumerRecord[K, V], V, NotUsed, Future[Done]] {

  private var control: Option[DrainingControl[Done]] = None

  override def processEnvelope(envelope: ConsumerRecord[K, V])(implicit ex: ExecutionContext): Future[Done] =
    handler.onEvent(envelope.value())

  override def start()(implicit ex: ExecutionContext, materializer: Materializer): Unit = {

    val drainingControl =
      consumerRecord
        .mapAsync(1) { envelope =>
          processEnvelope(envelope)
        }
        .toMat(Sink.ignore)(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()

    control = Some(drainingControl)
  }

  override def stop()(implicit ex: ExecutionContext): Future[Done] =
    control match {
      case Some(control) => control.drainAndShutdown().map(_ => Done)
      case _             => Future.successful(Done)
    }

}

class ConsumerRecordProjectionWithRunner[K, V, Result](
    sourceProvider: SourceProvider[Long, ConsumerRecord[K, V], Control],
    handler: EventHandler[V, Result],
    runner: ProjectionRunner[Long, Result])
    extends Projection[ConsumerRecord[K, V], V, Long, Result] {

  private var control: Option[Future[Consumer.DrainingControl[Done]]] = None

  def processEnvelope(envelope: ConsumerRecord[K, V])(implicit ex: ExecutionContext): Future[Done] =
    runner.run(envelope.offset()) { () =>
      handler.onEvent(envelope.value())
    }

  override def start()(implicit ex: ExecutionContext, materializer: Materializer): Unit = {

    val offsetFut = runner.offsetStore.readOffset()

    val drainingControl =
      Source
        .fromFutureSource(offsetFut.map(sourceProvider.source))
        .mapAsync(1)(processEnvelope)
        .toMat(Sink.ignore)(Keep.both)
        .mapMaterializedValue {
          case (futCtrl, futDone) =>
            futCtrl.map { ctrl =>
              DrainingControl(ctrl, futDone)
            }
        }
        .run()

    control = Some(drainingControl)
  }

  override def stop()(implicit ex: ExecutionContext): Future[Done] = {
    control match {
      case Some(control) => control.flatMap(_.drainAndShutdown()).map(_ => Done)
      case _             => Future.successful(Done)
    }
  }

}
