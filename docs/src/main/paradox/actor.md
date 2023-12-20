# Processing with Actor

A good alternative for advanced state management is to implement the handler as an [actor](https://doc.akka.io/docs/akka/current/typed/actors.html).

The following example is using the `CassandraProjection` but the handler and actor would be the same if used
any other @ref:[offset storage](overview.md). 

An actor `Behavior` for the word count example that was introduced in the section about @ref:[Stateful handler](cassandra.md#stateful-handler):

Scala
:  @@snip [WordCountDocExample.scala](/examples/src/test/scala/docs/cassandra/WordCountDocExample.scala) { #behaviorLoadingInitialState }

Java
:  @@snip [WordCountDocExample.java](/examples/src/test/java/jdocs/cassandra/WordCountDocExample.java) { #ActorHandler-imports #behaviorLoadingInitialState }

The handler can be definined as:

Scala
:  @@snip [WordCountDocExample.scala](/examples/src/test/scala/docs/cassandra/WordCountDocExample.scala) { #actorHandler }

Java
:  @@snip [WordCountDocExample.java](/examples/src/test/java/jdocs/cassandra/WordCountDocExample.java) { #actorHandler }

and the `Projection`:

Scala
:  @@snip [WordCountDocExample.scala](/examples/src/test/scala/docs/cassandra/WordCountDocExampleSpec.scala) { #actorHandlerProjection }

Java
:  @@snip [WordCountDocExample.java](/examples/src/test/java/jdocs/cassandra/WordCountDocExampleTest.java) { #actorHandlerProjection }

The `Behavior` given to the `ActorHandler` is spawned automatically by the `Projection` and each envelope is sent to
the actor with `ask`. The actor is supposed to send a response message to the `replyTo` when it has completed the
processing of the envelope. The @scala[`Try`]@java[`Optional<Throwable> error`] indicates if the processing was
successful or failed.

The lifecycle of the actor is managed by the `Projection`. The actor is automatically stopped when the `Projection` is stopped.

Another implementation that is loading the current count for a word on demand, and thereafter caches it in the
in-memory state: 

Scala
:  @@snip [WordCountDocExample.scala](/examples/src/test/scala/docs/cassandra/WordCountDocExample.scala) { #behaviorLoadingOnDemand }

Java
:  @@snip [WordCountDocExample.java](/examples/src/test/java/jdocs/cassandra/WordCountDocExample.java) { #behaviorLoadingOnDemand }   
