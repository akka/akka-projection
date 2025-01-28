/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb.internal

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._

import akka.Done
import akka.annotation.InternalApi
import akka.projection.dynamodb.javadsl
import akka.projection.dynamodb.scaladsl
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem

/**
 * INTERNAL API: Adapter from javadsl.DynamoDBTransactHandler to scaladsl.DynamoDBTransactHandler
 */
@InternalApi private[projection] class DynamoDBTransactHandlerAdapter[Envelope](
    delegate: javadsl.DynamoDBTransactHandler[Envelope])
    extends scaladsl.DynamoDBTransactHandler[Envelope] {

  override def process(envelope: Envelope): Future[Iterable[TransactWriteItem]] =
    delegate.process(envelope).asScala.map(_.asScala)(ExecutionContext.parasitic)

  override def start(): Future[Done] =
    delegate.start().asScala

  override def stop(): Future[Done] =
    delegate.stop().asScala
}

/**
 * INTERNAL API: Adapter from javadsl.DynamoDBTransactHandler to scaladsl.DynamoDBTransactHandler
 */
@InternalApi private[projection] class DynamoDBTransactGroupedHandlerAdapter[Envelope](
    delegate: javadsl.DynamoDBTransactHandler[java.util.List[Envelope]])
    extends scaladsl.DynamoDBTransactHandler[Seq[Envelope]] {

  override def process(envelopes: Seq[Envelope]): Future[Iterable[TransactWriteItem]] =
    delegate.process(envelopes.asJava).asScala.map(_.asScala)(ExecutionContext.parasitic)

  override def start(): Future[Done] =
    delegate.start().asScala

  override def stop(): Future[Done] =
    delegate.stop().asScala
}
