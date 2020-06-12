# Processing with Actor

A good alternative for advanced state management is to implement the handler as an [actor](https://doc.akka.io/docs/akka/current/typed/actors.html).

An actor `Behavior` for the word count example that was introduced in the section about @ref:[Stateful handler](cassandra.md#stateful-handler):

Scala
:  @@snip [WordCountDocExample.scala](/examples/src/test/scala/docs/cassandra/WordCountDocExample.scala) { #behaviorLoadingInitialState }

Java
:  @@snip [WordCountDocExample.java](/examples/src/test/java/jdocs/cassandra/WordCountDocExample.java) { #todo }

The handler can be definined as:

Scala
:  @@snip [WordCountDocExample.scala](/examples/src/test/scala/docs/cassandra/WordCountDocExample.scala) { #actorHandler }

Java
:  @@snip [WordCountDocExample.java](/examples/src/test/java/jdocs/cassandra/WordCountDocExample.java) { #todo }

and the `Projection`:

Scala
:  @@snip [WordCountDocExample.scala](/examples/src/test/scala/docs/cassandra/WordCountDocExampleSpec.scala) { #actorHandlerProjection }

Java
:  @@snip [WordCountDocExample.java](/examples/src/test/java/jdocs/cassandra/WordCountDocExample.java) { #todo }

The `Behavior` given to the `ActorHandler` is spawned automatically by the `Projection` and each envelope is sent to the actor
with the `envelopeMessage` factory defined for the `ActorHandler`. In addition to the envelope there is also
an @scala[`replyTo: ActorRef[Try[Done]]`]@java[`replyTo: ActorRef<FIXME>`] parameter in the `envelopeMessage` factory.
The actor is supposed to send a response message to that `ActorRef` when it has completed the processing of the
envelope. The @scala[`Try`]@java[FIXME] indicates if the processing was successful or failed.

Another implementation that is loading the current count for a word on demand, and thereafter caches it in the
in-memory state: 

Scala
:  @@snip [WordCountDocExample.scala](/examples/src/test/scala/docs/cassandra/WordCountDocExample.scala) { #behaviorLoadingOnDemand }

Java
:  @@snip [WordCountDocExample.java](/examples/src/test/java/jdocs/cassandra/WordCountDocExample.java) { #todo }   
