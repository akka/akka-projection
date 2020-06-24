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

A @apidoc:[SourceProvider] will provide the data to our projection. In Projections data is referenced as an `Envelope`, but it could reference an event, a message from a queueing system, or a record from a database table.
There are several supported Source Provider's available (or you can build your own), but in this example we will use the @ref:[Event Sourced Source Provider](eventsourced.md).

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
There are many kinds of handlers we can choose from.
This example uses a simple stateful Handler that will persist `ShoppingCartEvents.CheckedOut` events using the repo that was just created.
The example will also occassionally log the current daily count.
Because it's not critical that the Handler counter be accurate from the start of the projection we can keep it in a simple local volatile variable.
Every 10 events we will query the current daily count from the repository and log it.

Scala
:  @@snip [ShoppingCartApp.scala](/examples/src/test/scala/docs/guide/ShoppingCartApp.scala) { #guideProjectionHandler }

The projection is run by wrapping it in a @apidoc[ProjectionBehavior] and running it with a typed @apidoc[akka.actor.typed.ActorSystem].

Scala
:  @@snip [ShoppingCartApp.scala](/examples/src/test/scala/docs/guide/ShoppingCartApp.scala) { #guideProjectionSetup }

## Part 3: Writing tests for a Projection

## Part 4: Running a Projection

Create Cassandra offset schema
Schema: cassandra.md#schema

Create Cassandra projection schema

## Part 5: Adding error handling

## Part 6: Monitoring

## Part 7: Manage offsets

