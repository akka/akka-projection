/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit.internal

import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.function.Supplier

import scala.compat.java8.FunctionConverters._
import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.Future

import akka.NotUsed
import akka.annotation.InternalApi
import akka.projection.OffsetVerification
import akka.projection.testkit.scaladsl.TestSourceProvider
import akka.stream.scaladsl.Source

/**
 * INTERNAL API
 */
@InternalApi
private[projection] class TestSourceProviderImpl[Offset, Envelope] private[projection] (
    sourceEvents: Source[Envelope, NotUsed],
    extractOffsetFn: Envelope => Offset,
    extractCreationTimeFn: Envelope => Long,
    verifyOffsetFn: Offset => OffsetVerification,
    startSourceFromFn: (Offset, Offset) => Boolean,
    allowCompletion: Boolean)
    extends akka.projection.testkit.javadsl.TestSourceProvider[Offset, Envelope]
    with TestSourceProvider[Offset, Envelope] {

  private def copy(
      sourceEvents: Source[Envelope, NotUsed] = sourceEvents,
      extractOffsetFn: Envelope => Offset = extractOffsetFn,
      extractCreationTimeFn: Envelope => Long = extractCreationTimeFn,
      verifyOffsetFn: Offset => OffsetVerification = verifyOffsetFn,
      startSourceFromFn: (Offset, Offset) => Boolean = startSourceFromFn,
      allowCompletion: Boolean = allowCompletion): TestSourceProviderImpl[Offset, Envelope] =
    new TestSourceProviderImpl(
      sourceEvents,
      extractOffsetFn,
      extractCreationTimeFn,
      verifyOffsetFn,
      startSourceFromFn,
      allowCompletion)

  def withExtractCreationTimeFunction(
      extractCreationTimeFn: Envelope => Long): TestSourceProviderImpl[Offset, Envelope] =
    copy(extractCreationTimeFn = extractCreationTimeFn)

  def withExtractCreationTimeFunction(
      extractCreationTimeFn: java.util.function.Function[Envelope, Long]): TestSourceProviderImpl[Offset, Envelope] =
    withExtractCreationTimeFunction(extractCreationTimeFn.asScala)

  def withAllowCompletion(allowCompletion: Boolean): TestSourceProviderImpl[Offset, Envelope] =
    copy(allowCompletion = allowCompletion)

  def withOffsetVerification(verifyOffsetFn: Offset => OffsetVerification): TestSourceProviderImpl[Offset, Envelope] =
    copy(verifyOffsetFn = verifyOffsetFn)

  override def withOffsetVerification(offsetVerificationFn: java.util.function.Function[Offset, OffsetVerification])
      : TestSourceProviderImpl[Offset, Envelope] =
    withOffsetVerification(offsetVerificationFn.asScala)

  def withStartSourceFrom(startSourceFromFn: (Offset, Offset) => Boolean): TestSourceProviderImpl[Offset, Envelope] =
    copy(startSourceFromFn = startSourceFromFn)

  def withStartSourceFrom(startSourceFromFn: java.util.function.BiFunction[Offset, Offset, java.lang.Boolean])
      : TestSourceProviderImpl[Offset, Envelope] = {
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
