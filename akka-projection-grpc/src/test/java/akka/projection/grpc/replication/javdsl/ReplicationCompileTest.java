/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.javdsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.grpc.GrpcClientSettings;
import akka.grpc.javadsl.ServiceHandler;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.japi.function.Function;
import akka.persistence.query.Offset;
import akka.persistence.query.typed.EventEnvelope;
import akka.persistence.typed.ReplicaId;
import akka.persistence.typed.javadsl.ReplicatedEventSourcedBehavior;
import akka.persistence.typed.javadsl.ReplicationContext;
import akka.projection.ProjectionContext;
import akka.projection.ProjectionId;
import akka.projection.grpc.producer.EventProducerSettings;
import akka.projection.grpc.replication.javadsl.Replica;
import akka.projection.grpc.replication.javadsl.Replication;
import akka.projection.grpc.replication.javadsl.ReplicationProjectionProvider;
import akka.projection.grpc.replication.javadsl.ReplicationSettings;
import akka.projection.javadsl.SourceProvider;
import akka.projection.r2dbc.R2dbcProjectionSettings;
import akka.projection.r2dbc.javadsl.R2dbcProjection;
import akka.projection.javadsl.AtLeastOnceFlowProjection;
import akka.stream.javadsl.FlowWithContext;

import java.time.Duration;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;

public class ReplicationCompileTest {

 interface MyCommand {}

 static ReplicatedEventSourcedBehavior<MyCommand, Void, Void> create(ReplicationContext context) {
   throw new UnsupportedOperationException("just a dummy factory method");
 }

  public static void start(ActorSystem<?> system) {
   Set<Replica> otherReplicas = new HashSet<>();
   otherReplicas.add(Replica.create(
       ReplicaId.apply("DCB"),
       2,
       GrpcClientSettings.connectToServiceAt("b.example.com", 443, system).withTls(true)
       ));
   otherReplicas.add(Replica.create(
       ReplicaId.apply("DCC"),
       2,
       GrpcClientSettings.connectToServiceAt("c.example.com", 443, system).withTls(true)
   ));

   ReplicationProjectionProvider projectionProvider = new ReplicationProjectionProvider() {

    @Override
    public AtLeastOnceFlowProjection<Offset, EventEnvelope<Object>> create(ProjectionId projectionId, SourceProvider<Offset, EventEnvelope<Object>> sourceProvider, FlowWithContext<EventEnvelope<Object>, ProjectionContext, Done, ProjectionContext, NotUsed> replicationFlow, ActorSystem<?> system) {
     return  R2dbcProjection.atLeastOnceFlow(projectionId, Optional.<R2dbcProjectionSettings>empty(), sourceProvider, replicationFlow, system);
    }
   };

   // SAM so this is possible
   ReplicationProjectionProvider projectionProvider2 = (projectionId, sourceProvider, flow, s) ->
           R2dbcProjection.atLeastOnceFlow(projectionId, Optional.empty(), sourceProvider, flow, s);

   ReplicationSettings<MyCommand> settings = ReplicationSettings.<MyCommand>create(
       MyCommand.class,
       "my-entity",
       ReplicaId.apply("DCA"),
       EventProducerSettings.apply(system),
       otherReplicas,
       Duration.ofSeconds(10),
       // parallel updates
       8,
       projectionProvider);

   Replication<MyCommand> replication = Replication.grpcReplication(settings, ReplicationCompileTest::create, system);

   // bind a single handler endpoint
   Function<HttpRequest, CompletionStage<HttpResponse>> handler = replication.createSingleServiceHandler();

   @SuppressWarnings("unchecked")
   Function<HttpRequest, CompletionStage<HttpResponse>> service =
       ServiceHandler.concatOrNotFound(handler);

   CompletionStage<ServerBinding> bound =
       Http.get(system).newServerAt("127.0.0.1", 8080).bind(service);

  }
}
