/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.javdsl;/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

import akka.actor.typed.ActorSystem;
import akka.grpc.GrpcClientSettings;
import akka.grpc.javadsl.ServiceHandler;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.japi.function.Function;
import akka.persistence.typed.ReplicaId;
import akka.persistence.typed.javadsl.ReplicatedEventSourcedBehavior;
import akka.persistence.typed.javadsl.ReplicationContext;
import akka.projection.grpc.consumer.GrpcQuerySettings;
import akka.projection.grpc.producer.EventProducerSettings;
import akka.projection.grpc.replication.Replica;
import akka.projection.grpc.replication.ReplicationSettings;
import akka.projection.grpc.replication.javadsl.Replication;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

public class ReplicationCompileTest {

 interface MyCommand {}

 static ReplicatedEventSourcedBehavior<MyCommand, Void, Void> create(ReplicationContext context) {
   throw new UnsupportedOperationException("just a dummy factory method");
 }

  public static void start(ActorSystem<?> system) {
   Set<Replica> otherReplicas = new HashSet<>();
   otherReplicas.add(Replica.apply(
       ReplicaId.apply("DCB"),
       2,
       GrpcQuerySettings.create(system),
       GrpcClientSettings.connectToServiceAt("b.example.com", 443, system).withTls(true)
       ));
   otherReplicas.add(Replica.apply(
       ReplicaId.apply("DCC"),
       2,
       GrpcQuerySettings.create(system),
       GrpcClientSettings.connectToServiceAt("c.example.com", 443, system).withTls(true)
   ));
   ReplicationSettings<MyCommand> settings = ReplicationSettings.create(
       MyCommand.class,
       "my-entity",
       ReplicaId.apply("DCA"),
       EventProducerSettings.apply(system),
       otherReplicas);

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
