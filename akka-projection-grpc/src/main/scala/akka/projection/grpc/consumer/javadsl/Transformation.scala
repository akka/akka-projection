/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.consumer.javadsl

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.query.typed.EventEnvelope
import akka.projection.grpc.consumer.scaladsl

import java.util.{ Set => JSet }

import java.util.Optional
import java.util.function.{ Function => JFunction }

import scala.jdk.OptionConverters._
import scala.reflect.ClassTag
import scala.jdk.CollectionConverters._

object Transformation {

  /**
   * Starting point for building `Transformation`. Registrations of actual transformations must
   * be added. Use [[Transformation.identity]] to pass through each event as is.
   */
  val empty: Transformation = new Transformation(scaladsl.EventProducerPushDestination.Transformation.empty)

  /**
   * No transformation. Pass through each event as is.
   */
  val identity: Transformation =
    new Transformation(scaladsl.EventProducerPushDestination.Transformation.identity)

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] def adapted(delegate: scaladsl.EventProducerPushDestination.Transformation) =
    new Transformation(delegate)
}

/**
 * Transformation of incoming pushed events from the producer to the internal representation stored in the journal
 * and seen by local projections. Start from [[Transformation.empty]] when defining transformations.
 */
final class Transformation private (val delegate: scaladsl.EventProducerPushDestination.Transformation) {

  /**
   * Add or replace tags for incoming events
   */
  def registerTagMapper[A](inputEventClass: Class[A], f: JFunction[EventEnvelope[A], JSet[String]]): Transformation = {
    implicit val ct: ClassTag[A] = ClassTag(inputEventClass)
    new Transformation(delegate.registerTagMapper[A](envelope => f(envelope).asScala.toSet))
  }

  /**
   * Transform incoming persistence ids, care must be taken to produce a valid persistence id and to always map the
   * same incoming persistence id to the same stored persistence id to not introduce gaps in the sequence numbers
   * and break consuming projections.
   */
  def registerPersistenceIdMapper(system: ActorSystem[_], f: JFunction[EventEnvelope[Any], String]): Transformation = {
    new Transformation(delegate.registerPersistenceIdMapper(f.apply)(system))
  }

  /**
   * Transform incoming event payloads.
   *
   * Events can be excluded by mapping the payload to `Optional.empty`.
   */
  def registerMapper[A, B](inputEventClass: Class[A], f: JFunction[A, Optional[B]]): Transformation = {
    implicit val ct: ClassTag[A] = ClassTag(inputEventClass)
    new Transformation(delegate.registerEnvelopeMapper[A, B](envelope => f(envelope.event).toScala))
  }

  /**
   * Transform incoming event payloads, with access to the entire envelope for additional metadata.
   *
   * Events can be excluded by mapping the payload `Optional.empty`.
   */
  def registerEnvelopeMapper[A, B](
      inputEventClass: Class[A],
      f: JFunction[EventEnvelope[A], Optional[B]]): Transformation = {
    implicit val ct: ClassTag[A] = ClassTag(inputEventClass)
    new Transformation(delegate.registerEnvelopeMapper[A, B](envelope => f(envelope).toScala))
  }

  /**
   * Events can be excluded by mapping them to `Optional.empty`.
   */
  def registerOrElsePayloadMapper(f: JFunction[EventEnvelope[Any], Optional[Any]]): Transformation = {
    new Transformation(delegate.registerOrElsePayloadMapper(envelope => f(envelope).toScala))
  }

}
