/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

//package akka.projection.kafka.internal
//
//import java.util.Properties
//
//import scala.concurrent.Future
//import scala.concurrent.duration._
//import scala.jdk.CollectionConverters._
//import scala.jdk.DurationConverters._
//
//import akka.Done
//import akka.projection.ProjectionId
//import org.apache.kafka.clients.admin.AdminClient
//import org.apache.kafka.clients.admin.CreateTopicsOptions
//import org.apache.kafka.clients.admin.KafkaAdminClient
//import org.apache.kafka.clients.admin.NewTopic
//import org.apache.kafka.common.TopicPartition
//import org.apache.kafka.common.config.TopicConfig
//
//class KafkaOffsetStore {
//  // TODO: make configurable
//  val topic = "akka_projection"
//  val partitions = 1
//  val rf = 1
//
//  // TODO: see Alpakka Kafka for typesafe config to java properties conversion
//  val adminClientProperties = new Properties
//
//  def saveOffset(id: ProjectionId, offsets: Map[TopicPartition, Long]) = {}
//
//  def createTopic(): Future[Done] = Future {
//    val adminClient = AdminClient.create(adminClientProperties)
//    val newTopic = (new NewTopic(topic, partitions, rf))
//      .configs(Map(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT).asJava)
//    adminClient.createTopics(List(newTopic).asJavaCollection, new CreateTopicsOptions().timeoutMs(10.seconds.asJava))
//    akka.Done
//  }
//
//}
