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
import akka.projection.internal.MergeableOffsets
import akka.projection.kafka.KafkaSourceProvider
import akka.projection.scaladsl.SourceProvider
import akka.projection.slick.SlickProjection
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import slick.basic.DatabaseConfig
import slick.dbio.DBIOAction
import slick.jdbc.H2Profile

object KafkaToSlickExample {

  ActorSystem[Nothing](Guardian(), "KafkaToSlickExample")

  object Guardian {
    def apply(): Behavior[Nothing] = {
      Behaviors.setup[Nothing] { context =>
        implicit val system = context.system

        val config = ConfigFactory.parseString("""
        akka {
         loglevel = "DEBUG"
         projection.slick = {
  
           profile = "slick.jdbc.H2Profile$"
  
           # TODO: configure connection pool and slick async executor
           db = {
            url = "jdbc:h2:mem:test1"
            driver = org.h2.Driver
            connectionPool = disabled
            keepAliveConnection = true
           }
         }
        }
        """)
        val dbConfig: DatabaseConfig[H2Profile] = DatabaseConfig.forConfig("akka.projection.slick", config)
        val projectionId = ProjectionId("ExampleProjection", "ExampleProjection-1")
        val producerSettings = ProducerSettings(system, new StringSerializer, new ByteArraySerializer)
        val sendProducer = SendProducer(producerSettings)(system.toClassic)
        val consumerSettings = ConsumerSettings(system, new StringDeserializer, new ByteArrayDeserializer)
        val kafkaSourceProvider: SourceProvider[MergeableOffsets.Offset, ConsumerRecord[String, Array[Byte]]] =
          KafkaSourceProvider(consumerSettings, Subscriptions.topics("user-events"))

        val projection =
          SlickProjection.atLeastOnce(
            projectionId,
            sourceProvider = kafkaSourceProvider,
            dbConfig,
            saveOffsetAfterEnvelopes = 100,
            saveOffsetAfterDuration = 200.millis) { consumerRecord =>
            // do something with the record, payload in record.value
            val producerRecord = new ProducerRecord[String, Array[Byte]](
              "projection-target-topic",
              consumerRecord.key(),
              consumerRecord.value())
            // TODO: how do you flatmap together asynchronous stuff with the DBIO that has to be returned?
            sendProducer.send(producerRecord).mapTo[Done]
            DBIOAction.successful(Done)
          }

        projection.run()

        Behaviors.empty
      }
    }

  }
}
