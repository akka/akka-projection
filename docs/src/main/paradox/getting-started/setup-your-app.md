# Setup your application

Add the Akka Projections core library to a new project.
This isn't strictly required, because as we add other dependencies in the following steps it will transitively include core as a dependency, but it never hurts to be explicit.

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-projection-core_$scala.binary.version$
  version=$project.version$
}

Define the event type protocol that will represent each `Envelope` streamed from the Source Provider.
Create a @apidoc[akka.actor.typed.ActorSystem] for various Projections implementations to use.

Scala
:  @@snip [ShoppingCartApp.scala](/examples/src/test/scala/docs/guide/ShoppingCartApp.scala) { #guideSetup }
