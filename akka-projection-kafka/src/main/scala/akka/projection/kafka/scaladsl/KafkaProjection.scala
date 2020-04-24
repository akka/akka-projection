/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.kafka.scaladsl

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import akka.kafka.ProducerSettings
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.projection.internal.OffsetStore
import akka.projection.kafka.internal.KafkaProjectionImpl
import akka.projection.scaladsl.SourceProvider
import org.apache.kafka.clients.producer.ProducerRecord

class KafkaProjection {
  def atLeastOnce[Offset, Envelope, Key, Value](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      offsetStore: OffsetStore[Offset, Future],
      settings: ProducerSettings[Key, Value],
      saveOffsetAfterEnvelopes: Int,
      saveOffsetAfterDuration: FiniteDuration)(handler: Envelope => ProducerRecord[Key, Value]): Projection[Envelope] =
    new KafkaProjectionImpl(
      projectionId,
      sourceProvider,
      offsetStore,
      settings,
      KafkaProjectionImpl.AtLeastOnce(saveOffsetAfterEnvelopes, saveOffsetAfterDuration),
      handler)

  def atMostOnce[Offset, Envelope, Key, Value](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      offsetStore: OffsetStore[Offset, Future],
      settings: ProducerSettings[Key, Value])(handler: Envelope => ProducerRecord[Key, Value]): Projection[Envelope] =
    new KafkaProjectionImpl(
      projectionId,
      sourceProvider,
      offsetStore,
      settings,
      KafkaProjectionImpl.AtMostOnce,
      handler)
}
