# Getting Started Guide

By now you should understand the fundamental concepts of how a Projection works by reading about its @ref:[use cases](use-cases.md).
This guide will briefly describe the basic components of a Projection and instruct you step-by-step on how to build a functioning application.
The example used in this guide is based on a more complete application called `akka-sample-cqrs` that's found in [akka-samples](https://github.com/akka/akka-samples/). 
It builds a full @ref:[CQRS](use-cases.md#command-query-responsibility-segregation-cqrs-) (Command Query Responsibility Segregation) ES (Event Sourcing) system using a combination of features from the akka toolkit.

## Part 1: Setup your application

Add the Akka Projections core library to a new project.
This isn't strictly required, because as we add other dependencies in the following steps it will transitively include core as a dependency, but it never hurts to be explicit.

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-projection-core_$scala.binary.version$
  version=$project.version$
}

Define the event type protocol that will represent each `Envelope` streamed from the Source Provider.
Create a typed @apidoc[akka.actor.typed.ActorSystem] for various Projections implementations to use.

Scala
:  @@snip [ShoppingCartApp.scala](/examples/src/test/scala/docs/guide/ShoppingCartApp.scala) { #guideSetup }

## Part 1: Choosing a Source Provider

A @apidoc:[SourceProvider] will provide the data to our projection. 
In Projections each element that's processed is an `Envelope` and each `Envelope` contains an `Event`.
An `Envelope` must include an `Offset`, but it can also contain other information such as creation timestamp, a topic name, an entity tag, etc.
There are several supported Source Provider's available (or you can build your own), but in this example we will use the @ref:[Akka Persistence `EventSourced` Source Provider](eventsourced.md).

Add the following dependencies to your project:

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-projection-eventsourced_$scala.binary.version$
  version=$project.version$
}

Add the following imports to your project:

Scala
:  @@snip [ShoppingCartApp.scala](/examples/src/test/scala/docs/guide/ShoppingCartApp.scala) { #guideSourceProviderImports }

Create the @apidoc[SourceProvider].
The @ref:[Event Sourced Source Provider](eventsourced.md) is using [Akka Persistence](https://doc.akka.io/docs/akka/current/typed/persistence.html) internally (specifically the [eventsByTag](https://doc.akka.io/docs/akka/current/persistence-query.html#eventsbytag-and-currenteventsbytag) API).
To initialize the Source Provider we need to set parameters to choose the Akka Persistence plugin (Cassandra) to use as well as the name of the tag used for events we're interested in from the journal.

Scala
:  @@snip [ShoppingCartApp.scala](/examples/src/test/scala/docs/guide/ShoppingCartApp.scala) { #guideSourceProviderSetup }

## Part 2: Build a simple Projection handler

While building a projection there are several non-functional requirements to consider.
_What technology to project into? What message delivery semantics are acceptable for the system? Is it compatible with the chosen Source Provider to enable exactly-once message delivery? Does runtime state need to be maintained in the projection while it's running?_
It's up to the user to choose the right answers to these questions, but you must research if the answers to these questions compatible with each other.

For the purpose of this guide we will persist our Projection to Cassandra with at-least-once semantics and state will not be required in the projection handler itself.
To proceed we must add the Cassandra Projection implementation to our project:

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-projection-cassandra_$scala.binary.version$
  version=$project.version$
}

Add the following imports to your project:

Scala
:  @@snip [ShoppingCartApp.scala](/examples/src/test/scala/docs/guide/ShoppingCartApp.scala) { #guideProjectionImports }

It's the user's responsibility to implement the means to project into the target system, the Projection will only manage the persistence of the offset (though it is possible to enlist your projection into transactions when using projection implementations that support exactly-once like the @ref:[JDBC](jdbc.md)).
Write the code necessary to implement your projection.
For this example we are going filter `ShoppingCartEvents.CheckedOut` events from the Source Provider and write them to a Cassandra table that can be used to see the current daily count, as well as historical counts for the number of checked out shopping carts for each day of the year.
Implement a Cassandra repository that will manage a Cassandra table named `daily_checkouts`.

Scala
:  @@snip [ShoppingCartApp.scala](/examples/src/test/scala/docs/guide/ShoppingCartApp.scala) { #guideProjectionRepo }

Now it's time to write the Projection handler itself.
This example uses a simple stateful Handler that will persist `ShoppingCartEvents.CheckedOut` events using the repo that was just created.
The example will also occasionally log the current daily count.
Because it's not critical that the Handler counter be accurate from the start of the projection we can keep it in a simple local volatile variable.
Every 10 events we will query the current daily count from the repository and log it.

Scala
:  @@snip [ShoppingCartApp.scala](/examples/src/test/scala/docs/guide/ShoppingCartApp.scala) { #guideProjectionHandler }

The projection is run by wrapping it in a @apidoc[ProjectionBehavior] and running it with a typed @apidoc[akka.actor.typed.ActorSystem].

Scala
:  @@snip [ShoppingCartApp.scala](/examples/src/test/scala/docs/guide/ShoppingCartApp.scala) { #guideProjectionSetup }

## Part 3: Writing tests for a Projection

Like other akka libraries, Projections ships with a @ref:[TestKit](testing.md) that a user can include to assert their Projection handler implementation.
Add the Projections TestKit dependency to your project:

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-projection-testkit_$scala.binary.version$
  version=$project.version$
}

Import the @apidoc[ProjectionTestKit] and other utilities into a new [ScalaTest](https://www.scalatest.org/) test spec.

Scala
:  @@snip [ShoppingCartAppSpec.scala](/examples/src/test/scala/docs/guide/ShoppingCartAppSpec.scala) { #testKitImports }

The TestKit includes several utilities to run the Projection handler in isolation so that a full projection implementation and source provider are not required.

* @apidoc[ProjectionTestKit] takes an Akka @apidoc[akka.actor.testkit.typed.(javadsl|scaladsl).ActorTestKit] and runs a projection with the test @apidoc[akka.actor.typed.ActorSystem].
* @apidoc[TestSourceProvider] allows the user to mock out test data `Envelopes` that will be processed by the Projection Handler.
* @apidoc[TestProjection] is a test Projection implementation that uses an in-memory internal offset store.

Using these tools we can assert that our Projection handler meets the following requirements of the `DailyCheckoutProjectionHandler`.

// TODO: update for daily current item count
1. Only count `CheckedOut` shopping cart events.
1. Log every time we process increments of 10 `CheckedOut` in a single day.

Scala
:  @@snip [ShoppingCartAppSpec.scala](/examples/src/test/scala/docs/guide/ShoppingCartAppSpec.scala) { #testKitSpec }

## Part 4: Running a Projection

@@@ note

This example requires a Cassandra database to run. 
If you do not have a Cassandra database then you can run one locally as a Docker container.
To run a Cassandra database locally you can use [`docker-compose`](https://docs.docker.com/compose/) to run the [`docker-compose.yaml`](https://github.com/akka/akka-projection/blob/master/docker-compose.yml) found in the Projections project root.
The `docker-compose.yml` file references the latest [Cassandra Docker Image](https://hub.docker.com/_/cassandra).

```shell
$ docker-compose --project-name cassandra-projections up -d cassandra
Creating network "cassandra-projections_default" with the default driver
Creating cassandra-projections_cassandra_1 ... done
```

To get a `cqlsh` prompt run another Cassandra container in interactive mode using the same network (`cassandra-projections_default`) as the container currently running.

```shell
$ docker run -it --network cassandra-projections_default --rm cassandra cqlsh cassandra 
Connected to Test Cluster at cassandra:9042.
[cqlsh 5.0.1 | Cassandra 3.11.6 | CQL spec 3.4.4 | Native protocol v4]
Use HELP for help.
cqlsh> 
```

@@@

To run the Projection we must setup our Cassandra database to support the Cassandra Projection offset store as well as the new table we are projecting into with the `DailyCheckoutProjectionHandler`.

Create a Cassandra keyspace.

```
CREATE KEYSPACE IF NOT EXISTS akka_projection WITH REPLICATION = { 'class' : 'SimpleStrategy','replication_factor':1 };
```

Create the Cassandra Projection offset store table.
The DDL can be found in the @ref:[Cassandra Projection, Schema section](cassandra.md#schema).

Create the `DailyCheckoutProjectionHandler` projection table with the DDL found below.

```
CREATE TABLE IF NOT EXISTS akka_projection.daily_item_checkout_counts (
  date date,
  item_id text,
  checkout_count counter,
  PRIMARY KEY (date, item_id));
```



## Part 5: Adding error handling

## Part 6: Monitoring

## Part 7: Manage offsets

