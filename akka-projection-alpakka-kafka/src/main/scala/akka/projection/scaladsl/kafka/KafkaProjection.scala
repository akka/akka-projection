/*
 * Copyright (C) 2019-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl.kafka

import akka.Done
import akka.kafka.scaladsl.Consumer.{Control, DrainingControl}
import akka.projection.scaladsl._
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink, Source}

import scala.concurrent.{ExecutionContext, Future}

object KafkaProjection {

  def apply[E](src: Source[E, Control]): Projection =
    new KafkaProjection[E](src)
}

/**
 * A Projection implementation tailored for Alpakka Kafka.
 *
 * The materializer value must be a Control since the lifecycle of the stream will be managed by Akka Projection
 */
class KafkaProjection[E](kafkaSource: Source[E, Control]) extends Projection {

  private var control: Option[DrainingControl[Done]] = None

  override def start()(implicit ex: ExecutionContext, materializer: Materializer): Unit = {

    val drainingControl =
      kafkaSource
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
