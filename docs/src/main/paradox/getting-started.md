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

## Part 2: Build a Stateful Projection handler

While building a projection there are several non-functional requirements to consider.
_What technology to project into? What message delivery semantics are acceptable for the system? Is it compatible with the chosen Source Provider to enable exactly-once message delivery? Does runtime state need to be maintained in the projection while it's running?_
It's up to the user to choose the right answers to these questions, but you must research if the answers to these questions compatible with each other.

For this guide we will create a Projection that represents all checked out shopping carts and all the items that were purchased.
For the purpose of this guide we will persist our Projection to Cassandra with at-least-once semantics.
The Projection itself will be represented as a Cassandra table.

To proceed we must add the Cassandra Projection library to our project:

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-projection-cassandra_$scala.binary.version$
  version=$project.version$
}

Add the following imports to your project:

Scala
:  @@snip [ShoppingCartApp.scala](/examples/src/test/scala/docs/guide/ShoppingCartApp.scala) { #guideProjectionImports }

It's the user's responsibility to implement the means to project into the target system, the Projection itself will only manage the persistence of the offset (though it is possible to enlist your projection into transactions when using projection implementations that support exactly-once like the @ref:[JDBC](jdbc.md)).
This guide encapsulates its data access layer in a Repository called the `DailyCheckoutProjectionRepository`.
The repository will manage two Cassandra tables:

1. The `daily_checkouts` table will contain the actual Projection data of checked out shopping carts.
Each row in the table will include a date, shopping cart id, item id, and item quantity.
End users will be able to query the table by a date, cart id, or item id.
2. The `cart_state` table is an intermediate table that represents the current state of all active shopping carts.
This is required so that we can restore our state after the projection has been shutdown (i.e. a previously planned shutdown, a failure, or a rebalance).
This table can also be considered a Projection, even though we are only using it to represent intermediate state in this guide.

Scala
:  @@snip [ShoppingCartApp.scala](/examples/src/test/scala/docs/guide/ShoppingCartApp.scala) { #guideProjectionRepo }

Now it's time to write the Projection handler itself.
This example uses a @apidoc[StatefulHandler] that will process `ShoppingCartEvents.Event` events from the @apidoc[SourceProvider] that we implemented earlier.
Since this is a @apidoc[StatefulHandler] we will load our initial shopping cart state into memory by implementing `initialState`.
The event envelopes are processed in the `process` method. 
Each type of event results in a different action to take against the Projection tables.

This example will also occasionally log all the checked out carts and their contents every 10 checkout events that are processed.
The count of checkout events that are processed is represented with a mutable variable within the handler.
Because it's not critical that the logging counter be accurate from the start of the projection we can keep it in a simple local volatile variable.

Scala
:  @@snip [ShoppingCartApp.scala](/examples/src/test/scala/docs/guide/ShoppingCartApp.scala) { #guideProjectionHandler }

The projection is run by wrapping it in a @apidoc[ProjectionBehavior] and running it with a typed @apidoc[akka.actor.typed.ActorSystem].

Scala
:  @@snip [ShoppingCartApp.scala](/examples/src/test/scala/docs/guide/ShoppingCartApp.scala) { #guideProjectionSetup }

## Part 3: Writing tests for a Projection

Like other akka libraries, Projections ships with a @ref:[TestKit](testing.md) that a user can include to assert the correctness of their Projection handler implementation.
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

1. Build cart state correctly with cart add/modify/remove events and project cart state for every checkout.
1. Log the current checked out carts every time we process 10 checkout events.

Scala
:  @@snip [ShoppingCartAppSpec.scala](/examples/src/test/scala/docs/guide/ShoppingCartAppSpec.scala) { #testKitSpec }

To run the tests from the command line run the following sbt command.

```
sbt "examples/testOnly docs.guide.ShoppingCartAppSpec"
```

## Part 4: Running the Projection

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

To use a different Cassandra database update the [Cassandra driver's contact-points configuration](https://doc.akka.io/docs/akka-persistence-cassandra/current/configuration.html#contact-points-configuration) found in `./examples/src/resources/guide-shopping-cart-app.conf`.

@@@

To run the Projection we must setup our Cassandra database to support the Cassandra Projection offset store as well as the new tables we are projecting into with the `DailyCheckoutProjectionHandler`.

Create a Cassandra keyspace.

```
CREATE KEYSPACE IF NOT EXISTS akka_projection WITH REPLICATION = { 'class' : 'SimpleStrategy','replication_factor':1 };
```

Create the Cassandra Projection offset store table.
The DDL can be found in the @ref:[Cassandra Projection, Schema section](cassandra.md#schema).

Create the `DailyCheckoutProjectionHandler` projection tables with the DDL found below.

```
CREATE TABLE IF NOT EXISTS akka_projection.cart_state (
  cart_id text,
  item_id text,
  quantity int,
  PRIMARY KEY (cart_id, item_id));

CREATE TABLE IF NOT EXISTS akka_projection.daily_checkouts (
  date date,
  cart_id text,
  item_id text,
  quantity int,
  PRIMARY KEY (date, cart_id, item_id));
```

Source events are generated with the `EventGeneratorApp`.
This app is configured to use [Akka Persistence Cassandra](https://doc.akka.io/docs/akka-persistence-cassandra/current/index.html) to persist random `ShoppingCartApp.Events` to a journal.
It will checkout 10 shopping carts with random items and quantities every 10 seconds.
After 10 seconds it will increment the checkout time by 1 hour and repeat.
This app will also automatically create all the Akka Persistence infrastructure tables in the `akka` keyspace.
We won't go into any further detail about how this app functions because it falls outside the scope of Akka Projections.
To learn more about the writing events with [Akka Persistence see the Akka documentation](https://doc.akka.io/docs/akka/current/typed/index-persistence.html).

To run the `EventGeneratorApp` use the following sbt command.

```shell
sbt "examples/test:runMain docs.guide.EventGeneratorApp"
```

If you don't see any connection exceptions you should eventually see log lines produced with the event being written to the journal.

Ex)

<!-- FIXME: update when event generator app updated to persist to cart id persistenceids -->
```shell
[2020-07-16 15:13:59,855] [INFO] [docs.guide.EventGeneratorApp$] [] [EventGenerator-akka.actor.default-dispatcher-3] - persisting event ItemAdded(62e4e,bowling shoes,0) MDC: {persistencePhase=persist-evt, akkaAddress=akka://EventGenerator, akkaSource=akka://EventGenerator/user/persister, sourceActorSystem=EventGenerator, persistenceId=all-shopping-carts}
```

Finally, we can run the projection itself by using sbt to run `ShoppingCartApp`

```shell
sbt "examples/test:runMain docs.guide.ShoppingCartApp"
```

After a few seconds you should see the `DailyCheckoutProjectionHandler` logging that displays the current checkouts for the day:

```shell
[2020-07-16 15:26:23,420] [INFO] [docs.guide.DailyCheckoutProjectionHandler] [] [ShoppingCartApp-akka.actor.default-dispatcher-5] - DailyCheckoutProjectionHandler(carts-eu) current checkouts for the day [2020-07-16] is:                                                                                                 
Date        Cart ID  Item ID             Quantity                                                                                                             
2020-07-16  018db    akka t-shirt        0                                                                                                                    
2020-07-16  018db    cat t-shirt         2                                                                                                                    
2020-07-16  01ef3    akka t-shirt        1                                                                                                                    
2020-07-16  05747    bowling shoes       1                                                                                                                    
2020-07-16  064a0    cat t-shirt         1                                                                                                                    
2020-07-16  064a0    skis                0             
...
```

Use the CQL shell to observe the same information in the `daily_checkouts` table.

```
cqlsh:akka_projection> select cart_id, item_id, quantity from akka_projection.daily_checkouts where date = '2020-07-16' limit 10;

 cart_id | item_id       | quantity
---------+---------------+----------
   018db |  akka t-shirt |        0
   018db |   cat t-shirt |        2
   01ef3 |  akka t-shirt |        1
   03e7e | bowling shoes |        1
   03e7e |   cat t-shirt |        1
   05747 | bowling shoes |        1
   064a0 |   cat t-shirt |        1
   064a0 |          skis |        0
   06602 |          skis |        0
   084e1 |  akka t-shirt |        2

(10 rows)
```

<!--

## Part 5: Adding error handling

## Part 6: Monitoring

## Part 7: Manage offsets

-->