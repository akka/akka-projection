/**
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.grpc.producer.javadsl

import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.function.{ Function => JFunction }

import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.reflect.ClassTag

import akka.dispatch.ExecutionContexts
import akka.projection.grpc.producer.scaladsl

object Transformation {
  val empty: Transformation = new Transformation(
    scaladsl.EventProducer.Transformation.empty)

  /**
   * No transformation. Pass through each event as is.
   */
  val identity: Transformation = new Transformation(
    scaladsl.EventProducer.Transformation.identity)
}

/**
 * Transformation of events to the external (public) representation.
 * Events can be excluded by mapping them to `Optional.empty`.
 */
final class Transformation private (
    private[akka] val delegate: scaladsl.EventProducer.Transformation) {

  def registerAsyncMapper[A, B](
      inputEventClass: Class[A],
      f: JFunction[A, CompletionStage[Optional[B]]]): Transformation = {
    implicit val ct: ClassTag[A] = ClassTag(inputEventClass)
    new Transformation(delegate.registerAsyncMapper[A, B](event =>
      f.apply(event).toScala.map(_.asScala)(ExecutionContexts.parasitic)))
  }

  def registerMapper[A, B](
      inputEventClass: Class[A],
      f: JFunction[A, Optional[B]]): Transformation = {
    implicit val ct: ClassTag[A] = ClassTag(inputEventClass)
    new Transformation(
      delegate.registerMapper[A, B](event => f.apply(event).asScala))
  }

  def registerAsyncOrElseMapper(
      f: AnyRef => CompletionStage[Optional[AnyRef]]): Transformation = {
    new Transformation(
      delegate.registerAsyncOrElseMapper(event =>
        f.apply(event.asInstanceOf[AnyRef])
          .toScala
          .map(_.asScala)(ExecutionContexts.parasitic)))
  }

  def registerOrElseMapper(f: AnyRef => Optional[AnyRef]): Transformation = {
    new Transformation(delegate.registerOrElseMapper(event =>
      f.apply(event.asInstanceOf[AnyRef]).asScala))
  }
}
