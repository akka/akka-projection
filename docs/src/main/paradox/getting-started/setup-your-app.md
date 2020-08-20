# Setup your application

Add the Akka Projections core library to a new project.
This isn't strictly required, because as we add other dependencies in the following steps it will transitively include core as a dependency, but it never hurts to be explicit.

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-projection-core_$scala.binary.version$
  version=$project.version$
}

Define the event type protocol that will represent each `Envelope` streamed from the Source Provider.
Add `ShoppingCartEvents` to your project:

Scala
:  @@snip [ShoppingCartEvents.scala](/examples/src/test/scala/docs/guide/ShoppingCartEvents.scala) { #guideEvents }

Define the persistence tags to be used in your project.
Note that partitioned tags will be used later when @ref[running the projection in Akka Cluster](running-cluster.md).
Add `ShoppingCartTags` to your project:

Scala
:  @@snip [ShoppingCartTags.scala](/examples/src/test/scala/docs/guide/ShoppingCartTags.scala) { #guideTags }

Create the `ShoppingCartApp` with an `akka.actor.typed.ActorSystem` (API: @apidoc[akka.actor.typed.ActorSystem]) for Projections to use.
Create an empty [Guardian Actor](https://doc.akka.io/docs/akka/2.6/typed/actor-lifecycle.html#the-guardian-actor) (the root Actor of the `ActorSystem`).
We will populate this Actor in the following steps of the guide.
Note that we are using the `docs.scaladsl` package.
You may use any package, but this package is used in following steps when referencing the main class to run the app built with this guide.

Scala
:  @@snip [ShoppingCartApp.scala](/examples/src/test/scala/docs/guide/ShoppingCartApp.scala) { #guideSetup }
