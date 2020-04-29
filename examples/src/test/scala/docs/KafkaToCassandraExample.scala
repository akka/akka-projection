/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */
package docs

import scala.concurrent.duration._

import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.kafka.ConsumerSettings
import akka.kafka.ProducerSettings
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.SendProducer
import akka.projection.ProjectionId
import akka.projection.cassandra.scaladsl.CassandraProjection
import akka.projection.kafka.KafkaSourceProvider
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer

object KafkaToCassandraExample {

  ActorSystem[Nothing](Guardian(), "KafkaToCassandraExample")

  object Guardian {
    def apply(): Behavior[Nothing] = {
      Behaviors.setup[Nothing] { context =>
        implicit val system = context.system

        val projectionId = ProjectionId("ExampleProjection", "ExampleProjection-1")

        val producerSettings = ProducerSettings(system, new StringSerializer, new ByteArraySerializer)
        val sendProducer = SendProducer(producerSettings)(system.toClassic)

        val consumerSettings = ConsumerSettings(system, new StringDeserializer, new ByteArrayDeserializer)
        val projection = CassandraProjection.atLeastOnce(
          projectionId,
          sourceProvider = KafkaSourceProvider(consumerSettings, Subscriptions.topics("user-events")),
          saveOffsetAfterEnvelopes = 100,
          saveOffsetAfterDuration = 200.millis) { consumerRecord =>
          // do something with the record, payload in record.value
          val producerRecord = new ProducerRecord[String, Array[Byte]](
            "projection-target-topic",
            consumerRecord.key(),
            consumerRecord.value())
          sendProducer.send(producerRecord).mapTo[Done]
        }

        projection.run()

        Behaviors.empty
      }
    }

  }
}
