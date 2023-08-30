/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer.javadsl

import akka.persistence.query.typed.EventEnvelope
import akka.projection.grpc.consumer.scaladsl

import java.util.{ Set => JSet }
import akka.util.ccompat.JavaConverters._

import java.util.Optional
import java.util.function.{ Function => JFunction }
import scala.compat.java8.OptionConverters._
import scala.reflect.ClassTag

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
  def registerPersistenceIdMapper(f: JFunction[EventEnvelope[Any], String]): Transformation = {
    new Transformation(delegate.registerPersistenceIdMapper(f.apply))
  }

  /**
   * Events can be excluded by mapping the payload `Optional.empty`.
   */
  def registerPayloadMapper[A, B](
      inputEventClass: Class[A],
      f: JFunction[EventEnvelope[A], Optional[B]]): Transformation = {
    implicit val ct: ClassTag[A] = ClassTag(inputEventClass)
    new Transformation(delegate.registerPayloadMapper[A, B](envelope => f(envelope).asScala))
  }

  /**
   * Events can be excluded by mapping them to `Optional.empty`.
   */
  def registerOrElsePayloadMapper(f: JFunction[EventEnvelope[Any], Optional[Any]]): Transformation = {
    new Transformation(delegate.registerOrElsePayloadMapper(envelope => f(envelope).asScala))
  }

}
