/*
 * Copyright (C) 2022-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.producer.javadsl

import akka.annotation.ApiMayChange
import akka.dispatch.ExecutionContexts
import akka.persistence.query.typed.EventEnvelope
import akka.projection.grpc.producer.scaladsl

import java.util.Optional
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.function.{ Function => JFunction }
import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.reflect.ClassTag

@ApiMayChange
@FunctionalInterface
trait Mapper[A, B] {
  def apply(event: A, metadata: Optional[Any]): CompletionStage[Optional[B]]

}

@ApiMayChange
object Transformation {

  /**
   * Starting point for building `Transformation`. Registrations of actual transformations must
   * be added. Use [[Transformation.identity]] to pass through each event as is.
   */
  val empty: Transformation = new Transformation(scaladsl.EventProducer.Transformation.empty)

  /**
   * No transformation. Pass through each event as is.
   */
  val identity: Transformation = new Transformation(scaladsl.EventProducer.Transformation.identity)

}

/**
 * Transformation of events to the external (public) representation.
 * Events can be excluded by mapping them to `Optional.empty`.
 *
 * Not for direct construction, use [[Transformation.empty]] as starting point and register
 * mappers to build your needed Transformation
 */
@ApiMayChange
final class Transformation private[akka] (private[akka] val delegate: scaladsl.EventProducer.Transformation) {

  /**
   * @param f A function that is fed each event payload of type `A` and returns an
   *          async payload to emit, or `Optional.empty()` to filter the event from being produced.
   */
  def registerAsyncMapper[A, B](
      inputEventClass: Class[A],
      f: JFunction[A, CompletionStage[Optional[B]]]): Transformation = {
    implicit val ct: ClassTag[A] = ClassTag(inputEventClass)
    new Transformation(
      delegate.registerAsyncMapper[A, B](event => f.apply(event).toScala.map(_.asScala)(ExecutionContexts.parasitic)))
  }

  /**
   * @param f A function that is fed each event payload of type `A` and returns a
   *          payload to emit, or `Optional.empty()` to filter the event from being produced.
   */
  def registerMapper[A, B](inputEventClass: Class[A], f: JFunction[A, Optional[B]]): Transformation = {
    implicit val ct: ClassTag[A] = ClassTag(inputEventClass)
    new Transformation(delegate.registerMapper[A, B](event => f.apply(event).asScala))
  }

  /**
   * @param f A function that is fed each event envelope for payloads of type `A` and returns a
   *          payload to emit, or `Optional.empty()` to filter the event from being produced.
   */
  def registerEnvelopeMapper[A, B](
      inputEventClass: Class[A],
      f: JFunction[EventEnvelope[A], Optional[B]]): Transformation = {
    registerAsyncEnvelopeMapper[A, B](inputEventClass, envelope => CompletableFuture.completedFuture(f(envelope)))
  }

  /**
   * @param f A function that is fed each event envelope for payloads of type `A` and returns an
   *          async payload to emit, or `Optional.empty()` to filter the event from being produced.
   */
  def registerAsyncEnvelopeMapper[A, B](
      inputEventClass: Class[A],
      f: JFunction[EventEnvelope[A], CompletionStage[Optional[B]]]): Transformation = {
    implicit val ct: ClassTag[A] = ClassTag(inputEventClass)
    new Transformation(delegate.registerAsyncEnvelopeMapper[A, B](envelope =>
      f.apply(envelope).toScala.map(_.asScala)(ExecutionContexts.parasitic)))
  }

  /**
   * @param f A function that is fed each event payload, that did not match any other registered mappers, returns an
   *          async payload to emit, or `Optional.empty()` to filter the event from being produced. Replaces any previous "orElse"
   *          mapper defined.
   */
  def registerAsyncOrElseMapper(f: AnyRef => CompletionStage[Optional[AnyRef]]): Transformation = {
    new Transformation(
      delegate.registerAsyncOrElseMapper(
        event =>
          f.apply(event.asInstanceOf[AnyRef])
            .toScala
            .map(_.asScala)(ExecutionContexts.parasitic)))
  }

  /**
   * @param f A function that is fed each event payload, that did not match any other registered mappers, returns a
   *          payload to emit, or `Optional.empty()` to filter the event from being produced. Replaces any previous "orElse"
   *          mapper defined.
   */
  def registerOrElseMapper(f: AnyRef => Optional[AnyRef]): Transformation = {
    new Transformation(delegate.registerOrElseMapper(event => f.apply(event.asInstanceOf[AnyRef]).asScala))
  }

  /**
   * @param f A function that is fed each event envelope, that did not match any other registered mappers, returns an
   *          async payload to emit, or `Optional.empty()` to filter the event from being produced. Replaces any previous "orElse"
   *          mapper defined.
   */
  def registerAsyncEnvelopeOrElseMapper(
      f: JFunction[EventEnvelope[Any], CompletionStage[Optional[Any]]]): Transformation = {
    new Transformation(delegate.registerAsyncEnvelopeOrElseMapper(envelope =>
      f.apply(envelope).toScala.map(_.asScala)(ExecutionContexts.parasitic)))
  }
}
