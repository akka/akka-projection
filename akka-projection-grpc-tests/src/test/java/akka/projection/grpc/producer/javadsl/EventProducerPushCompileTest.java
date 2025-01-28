/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.producer.javadsl;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.Props;
import akka.actor.typed.SpawnProtocol;
import akka.grpc.GrpcClientSettings;
import akka.persistence.query.Offset;
import akka.persistence.query.typed.EventEnvelope;
import akka.persistence.r2dbc.query.javadsl.R2dbcReadJournal;
import akka.projection.ProjectionBehavior;
import akka.projection.ProjectionId;
import akka.projection.eventsourced.javadsl.EventSourcedProvider;
import akka.projection.grpc.producer.EventProducerSettings;
import akka.projection.javadsl.SourceProvider;
import akka.projection.r2dbc.javadsl.R2dbcProjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class EventProducerPushCompileTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventProducerPushCompileTest.class);



  public static void initProducer(ActorSystem<SpawnProtocol.Command> system) {
    // #producerSetup
    EventProducerPush<String> eventProducer = EventProducerPush.create(
        "producer-id",
        new EventProducerSource("entityTypeKeyName", "stream-id", Transformation.empty(), EventProducerSettings.create(system)),
        GrpcClientSettings.connectToServiceAt("localhost", 8080, system).withTls(false));
    SourceProvider<Offset, EventEnvelope<String>> eventSourcedProvider =
        EventSourcedProvider.eventsBySlices(
        system,
        R2dbcReadJournal.Identifier(),
        eventProducer.eventProducerSource().entityType(),
        0,
        1023);
    Behavior<ProjectionBehavior.Command> projectionBehavior = ProjectionBehavior.create(
        R2dbcProjection.atLeastOnceFlow(
            ProjectionId.of("producer", "0-1023"),
            Optional.empty(),
            eventSourcedProvider,
            eventProducer.handler(system),
            system));
    // #producerSetup

    system.tell(new SpawnProtocol.Spawn<>(
        projectionBehavior,
        "producer-projection",
        Props.empty(),
        system.ignoreRef().narrow()));

  }
}
