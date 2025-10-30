/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://akka.io>
 */

package jdocs.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.projection.MergeableOffset;
import akka.projection.Projection;
import akka.projection.ProjectionId;
import akka.projection.javadsl.ExactlyOnceProjection;
import akka.projection.javadsl.Handler;
import akka.projection.javadsl.SourceProvider;
import akka.projection.jdbc.javadsl.JdbcHandler;
import akka.projection.jdbc.javadsl.JdbcProjection;
import akka.projection.kafka.javadsl.KafkaSourceProvider;
import akka.stream.javadsl.Source;
import jdocs.jdbc.HibernateSessionFactory;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// #imports
import akka.kafka.ConsumerSettings;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

// #imports

// #imports-producer
import org.apache.kafka.common.serialization.StringSerializer;
import akka.kafka.ProducerSettings;
// #imports-producer

// #sendProducer
import akka.kafka.javadsl.SendProducer;

// #sendProducer

// #producerFlow
import org.apache.kafka.clients.producer.ProducerRecord;
import akka.kafka.ProducerMessage;
import akka.kafka.javadsl.Producer;
import akka.stream.javadsl.FlowWithContext;
import akka.projection.ProjectionContext;

// #producerFlow

import jdocs.jdbc.HibernateJdbcSession;

import jakarta.persistence.EntityManager;

public interface KafkaDocExample {

  // #todo
  // TODO
  // #todo

  // #handler
  public class WordCountHandler
      extends JdbcHandler<ConsumerRecord<String, String>, HibernateJdbcSession> {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final ProjectionId projectionId;

    private final Map<String, Integer> state = new HashMap<>();

    public WordCountHandler(ProjectionId projectionId) {
      this.projectionId = projectionId;
    }

    @Override
    public void process(HibernateJdbcSession session, ConsumerRecord<String, String> envelope) {
      String word = envelope.value();
      int newCount = state.getOrDefault(word, 0) + 1;
      logger.info(
          "{} consumed from topic/partition {}/{}. Word count for [{}] is {}",
          projectionId,
          envelope.topic(),
          envelope.partition(),
          word,
          newCount);

      state.put(word, newCount);
    }
  }
  // #handler

  // #wordSource
  public class WordEnvelope {
    public final Long offset;
    public final String word;

    public WordEnvelope(Long offset, String word) {
      this.offset = offset;
      this.word = word;
    }
  }

  class WordSource extends SourceProvider<Long, WordEnvelope> {

    private final Source<WordEnvelope, NotUsed> src =
        Source.from(
            Arrays.asList(
                new WordEnvelope(1L, "abc"),
                new WordEnvelope(2L, "def"),
                new WordEnvelope(3L, "ghi"),
                new WordEnvelope(4L, "abc")));

    @Override
    public CompletionStage<Source<WordEnvelope, NotUsed>> source(
        Supplier<CompletionStage<Optional<Long>>> offset) {
      return offset
          .get()
          .thenApply(
              o -> {
                if (o.isPresent())
                  return src.dropWhile(envelope -> envelope.offset <= o.get())
                      .throttle(1, Duration.ofSeconds(1));
                else return src.throttle(1, Duration.ofSeconds(1));
              });
    }

    @Override
    public Long extractOffset(WordEnvelope envelope) {
      return envelope.offset;
    }

    @Override
    public long extractCreationTime(WordEnvelope envelope) {
      return 0L;
    }
  }
  // #wordSource

  // #wordPublisher
  class WordPublisher extends Handler<WordEnvelope> {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final String topic;
    private final SendProducer<String, String> sendProducer;

    public WordPublisher(String topic, SendProducer<String, String> sendProducer) {
      this.topic = topic;
      this.sendProducer = sendProducer;
    }

    @Override
    public CompletionStage<Done> process(WordEnvelope envelope) {
      String word = envelope.word;
      // using the word as the key and `DefaultPartitioner` will select partition based on the key
      // so that same word always ends up in same partition
      String key = word;
      ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, word);
      CompletionStage<RecordMetadata> result = sendProducer.send(producerRecord);
      CompletionStage<Done> done =
          result.thenApply(
              recordMetadata -> {
                logger.info(
                    "Published word [{}] to topic/partition {}/{}",
                    word,
                    topic,
                    recordMetadata.partition());
                return Done.getInstance();
              });
      return done;
    }
  }
  // #wordPublisher

  static SourceProvider<MergeableOffset<Long>, ConsumerRecord<String, String>>
      illustrateSourceProvider() {
    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");

    // #sourceProvider
    String bootstrapServers = "localhost:9092";
    String groupId = "group-wordcount";
    String topicName = "words";
    ConsumerSettings<String, String> consumerSettings =
        ConsumerSettings.create(system, new StringDeserializer(), new StringDeserializer())
            .withBootstrapServers(bootstrapServers)
            .withGroupId(groupId)
            .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    SourceProvider<MergeableOffset<Long>, ConsumerRecord<String, String>> sourceProvider =
        KafkaSourceProvider.create(system, consumerSettings, Collections.singleton(topicName));
    // #sourceProvider

    return sourceProvider;
  }

  static void illustrateExactlyOnce() {
    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");
    SourceProvider<MergeableOffset<Long>, ConsumerRecord<String, String>> sourceProvider =
        illustrateSourceProvider();
    WordRepository wordRepository = null;

    // #exactlyOnce
    final HibernateSessionFactory sessionProvider = new HibernateSessionFactory();

    ProjectionId projectionId = ProjectionId.of("WordCount", "wordcount-1");
    ExactlyOnceProjection<MergeableOffset<Long>, ConsumerRecord<String, String>> projection =
        JdbcProjection.exactlyOnce(
            projectionId,
            sourceProvider,
            sessionProvider::newInstance,
            () -> new WordCountJdbcHandler(wordRepository),
            system);
    // #exactlyOnce
  }

  // #exactly-once-jdbc-handler
  public class WordCountJdbcHandler
      extends JdbcHandler<ConsumerRecord<String, String>, HibernateJdbcSession> {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private WordRepository wordRepository;

    public WordCountJdbcHandler(WordRepository wordRepository) {
      this.wordRepository = wordRepository;
    }

    @Override
    public void process(HibernateJdbcSession session, ConsumerRecord<String, String> envelope)
        throws Exception {
      String word = envelope.value();
      wordRepository.increment(session.entityManager, word);
    }
  }
  // #exactly-once-jdbc-handler

  // #repository
  interface WordRepository {
    void increment(EntityManager entityManager, String word);
  }
  // #repository

  static void IllustrateSendingToKafka() {
    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");

    // #sendProducer
    String bootstrapServers = "localhost:9092";
    String topicName = "words";
    ProducerSettings<String, String> producerSettings =
        ProducerSettings.create(system, new StringSerializer(), new StringSerializer())
            .withBootstrapServers(bootstrapServers);
    SendProducer<String, String> sendProducer = new SendProducer<>(producerSettings, system);
    // #sendProducer

    // #sendToKafkaProjection
    WordSource sourceProvider = new WordSource();
    HibernateSessionFactory sessionProvider = new HibernateSessionFactory();

    ProjectionId projectionId = ProjectionId.of("PublishWords", "words");
    Projection<WordEnvelope> projection =
        JdbcProjection.atLeastOnceAsync(
            projectionId,
            sourceProvider,
            sessionProvider::newInstance,
            () -> new WordPublisher(topicName, sendProducer),
            system);
    // #sendToKafkaProjection
  }

  static void IllustrateSendingToKafkaFlow() {
    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");

    // #producerFlow
    String bootstrapServers = "localhost:9092";
    String topicName = "words";

    ProducerSettings<String, String> producerSettings =
        ProducerSettings.create(system, new StringSerializer(), new StringSerializer())
            .withBootstrapServers(bootstrapServers);

    FlowWithContext<WordEnvelope, ProjectionContext, Done, ProjectionContext, NotUsed>
        producerFlow =
            FlowWithContext.<WordEnvelope, ProjectionContext>create()
                .map(
                    wordEnv ->
                        ProducerMessage.single(
                            new ProducerRecord<String, String>(
                                topicName, wordEnv.word, wordEnv.word)))
                .via(Producer.flowWithContext(producerSettings))
                .map(__ -> Done.getInstance());

    // #producerFlow

    // #sendToKafkaProjectionFlow
    WordSource sourceProvider = new WordSource();
    HibernateSessionFactory sessionProvider = new HibernateSessionFactory();

    ProjectionId projectionId = ProjectionId.of("PublishWords", "words");
    Projection<WordEnvelope> projection =
        JdbcProjection.atLeastOnceFlow(
            projectionId, sourceProvider, sessionProvider::newInstance, producerFlow, system);
    // #sendToKafkaProjectionFlow
  }
}
