/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit

import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.function.Supplier

import scala.compat.java8.FunctionConverters._
import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.Future

import akka.NotUsed
import akka.annotation.ApiMayChange
import akka.projection.OffsetVerification
import akka.projection.scaladsl.VerifiableSourceProvider
import akka.stream.scaladsl.Source

@ApiMayChange
object TestSourceProvider {

  /**
   * A [[TestSourceProvider]] is used to supply an arbitrary stream of envelopes to a [[TestProjection]]
   *
   * @param sourceEvents - a [[akka.stream.scaladsl.Source]] of envelopes
   * @param extractOffset - a user-defined function to extract the offset from an envelope.
   */
  def apply[Offset, Envelope](
      sourceEvents: Source[Envelope, NotUsed],
      extractOffset: Envelope => Offset): TestSourceProvider[Offset, Envelope] = {
    new TestSourceProvider[Offset, Envelope](
      sourceEvents = sourceEvents,
      extractOffsetFn = extractOffset,
      extractCreationTimeFn = (_: Envelope) => 0L,
      verifyOffsetFn = (_: Offset) => OffsetVerification.VerificationSuccess,
      startSourceFromFn = (_: Offset, _: Offset) => false,
      allowCompletion = false)
  }

  /**
   * A [[TestSourceProvider]] is used to supply an arbitrary stream of envelopes to a [[TestProjection]]
   *
   * @param sourceEvents - a [[akka.stream.javadsl.Source]] of envelopes
   * @param extractOffset - a user-defined function to extract the offset from an envelope
   */
  def create[Offset, Envelope](
      sourceEvents: akka.stream.javadsl.Source[Envelope, NotUsed],
      extractOffset: java.util.function.Function[Envelope, Offset]): TestSourceProvider[Offset, Envelope] =
    apply(sourceEvents.asScala, extractOffset.asScala)
}

@ApiMayChange
class TestSourceProvider[Offset, Envelope] private[projection] (
    sourceEvents: Source[Envelope, NotUsed],
    extractOffsetFn: Envelope => Offset,
    extractCreationTimeFn: Envelope => Long,
    verifyOffsetFn: Offset => OffsetVerification,
    startSourceFromFn: (Offset, Offset) => Boolean,
    allowCompletion: Boolean)
    extends akka.projection.javadsl.VerifiableSourceProvider[Offset, Envelope]
    with VerifiableSourceProvider[Offset, Envelope] {

  private def copy(
      sourceEvents: Source[Envelope, NotUsed] = sourceEvents,
      extractOffsetFn: Envelope => Offset = extractOffsetFn,
      extractCreationTimeFn: Envelope => Long = extractCreationTimeFn,
      verifyOffsetFn: Offset => OffsetVerification = verifyOffsetFn,
      startSourceFromFn: (Offset, Offset) => Boolean = startSourceFromFn,
      allowCompletion: Boolean = allowCompletion): TestSourceProvider[Offset, Envelope] =
    new TestSourceProvider(
      sourceEvents,
      extractOffsetFn,
      extractCreationTimeFn,
      verifyOffsetFn,
      startSourceFromFn,
      allowCompletion)

  /**
   * A user-defined function to extract the event creation time from an envelope.
   */
  def withExtractCreationTimeFunction(extractCreationTimeFn: Envelope => Long): TestSourceProvider[Offset, Envelope] =
    copy(extractCreationTimeFn = extractCreationTimeFn)

  /**
   * Java API
   *
   * A user-defined function to extract the event creation time from an envelope.
   */
  def withExtractCreationTimeFunction(
      extractCreationTimeFn: java.util.function.Function[Envelope, Long]): TestSourceProvider[Offset, Envelope] =
    withExtractCreationTimeFunction(extractCreationTimeFn.asScala)

  /**
   * Allow the [[sourceEvents]] Source to complete or stay open indefinitely.
   */
  def withAllowCompletion(allowCompletion: Boolean): TestSourceProvider[Offset, Envelope] =
    copy(allowCompletion = allowCompletion)

  /**
   * A user-defined function to verify offsets.
   */
  def withOffsetVerification(verifyOffsetFn: Offset => OffsetVerification): TestSourceProvider[Offset, Envelope] =
    copy(verifyOffsetFn = verifyOffsetFn)

  /**
   * Java API: A user-defined function to verify offsets.
   */
  def withOffsetVerification(offsetVerificationFn: java.util.function.Function[Offset, OffsetVerification])
      : TestSourceProvider[Offset, Envelope] =
    withOffsetVerification(offsetVerificationFn.asScala)

  /**
   * A user-defined function to compare the last offset returned by the offset store with the offset in the source
   * to filter out previously processed offsets.
   *
   * First parameter: Last offset processed. Second parameter this envelope's offset from [[sourceEvents]].
   */
  def withStartSourceFrom(startSourceFromFn: (Offset, Offset) => Boolean): TestSourceProvider[Offset, Envelope] =
    copy(startSourceFromFn = startSourceFromFn)

  /**
   * Java API: A user-defined function to compare the last offset returned by the offset store with the offset in the source
   * to filter out previously processed offsets.
   *
   * First parameter: Last offset processed. Second parameter this envelope's offset from [[sourceEvents]].
   */
  def withStartSourceFrom(startSourceFromFn: java.util.function.BiFunction[Offset, Offset, java.lang.Boolean])
      : TestSourceProvider[Offset, Envelope] = {
    val adapted: (Offset, Offset) => Boolean = (lastOffset, offset) =>
      Boolean.box(startSourceFromFn.apply(lastOffset, offset))
    withStartSourceFrom(adapted)
  }

  override def source(offset: () => Future[Option[Offset]]): Future[Source[Envelope, NotUsed]] = {
    implicit val ec = akka.dispatch.ExecutionContexts.parasitic
    val src =
      if (allowCompletion) sourceEvents
      else sourceEvents.concat(Source.maybe)
    offset().map {
      case Some(o) => src.dropWhile(env => startSourceFromFn(o, extractOffset(env)))
      case _       => src
    }
  }

  override def source(offset: Supplier[CompletionStage[Optional[Offset]]])
      : CompletionStage[akka.stream.javadsl.Source[Envelope, NotUsed]] = {
    implicit val ec = akka.dispatch.ExecutionContexts.parasitic
    source(() => offset.get().toScala.map(_.asScala)).map(_.asJava).toJava
  }

  override def extractOffset(envelope: Envelope): Offset = extractOffsetFn(envelope)

  override def extractCreationTime(envelope: Envelope): Long = extractCreationTimeFn(envelope)

  override def verifyOffset(offset: Offset): OffsetVerification = verifyOffsetFn(offset)
}
