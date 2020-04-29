/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */
package docs

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.kafka.ConsumerSettings
import akka.projection.ProjectionId
import akka.projection.cassandra.scaladsl.CassandraProjection
import akka.projection.kafka.KafkaSourceProvider
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer

object KafkaToCassandraExample {

  ActorSystem[Nothing](Guardian(), "KafkaToCassandraExample")

  object Guardian {
    def apply(): Behavior[Nothing] = {
      Behaviors.setup[Nothing] { context =>
        implicit val system = context.system

        val projectionId = ProjectionId("ExampleProjection", "ExampleProjection-1")
        val consumerSettings = ConsumerSettings(system, new StringDeserializer, new ByteArrayDeserializer)
        val projection = CassandraProjection.atLeastOnce(
          projectionId,
          sourceProvider = KafkaSourceProvider.fromOffsets(consumerSettings),
          saveOffsetAfterEnvelopes = 100,
          saveOffsetAfterDuration = 200.millis) { record =>
          // do something with the record, payload in record.value
          Future.successful(Done)
        }

        projection.run()

        Behaviors.empty
      }
    }

  }
}
