# Setup your application

Add the Akka Projections core library to a new project.
This isn't strictly required, because as we add other dependencies in the following steps it will transitively include core as a dependency, but it never hurts to be explicit.

The Akka dependencies are available from Akka's library repository. To access them there, you need to configure the URL for this repository.

@@repository [Maven,sbt,Gradle] {
id="akka-repository"
name="Akka library repository"
url="https://repo.akka.io/maven"
}

Additionally, add the dependency as below.

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-projection-core_$scala.binary.version$
  version=$project.version$
}

Define the event type protocol that will represent each `Envelope` streamed from the Source Provider.
Add `ShoppingCartEvents` to your project:

Scala
:  @@snip [ShoppingCartEvents.scala](/examples/src/test/scala/docs/guide/ShoppingCartEvents.scala) { #guideEvents }

Java
:  @@snip [ShoppingCartEvents.java](/examples/src/test/java/jdocs/guide/ShoppingCartEvents.java) { #guideEvents }

To enable serialization and deserialization of events with Akka Persistence it's necessary to define a base type for your event type hierarchy.
In this guide we are using [Jackson Serialization](https://doc.akka.io/docs/akka/current/serialization-jackson.html).
Add the `CborSerializable` base type to your project:

Scala
:  @@snip [CborSerializable.scala](/examples/src/test/scala/docs/guide/CborSerializable.scala) { #guideCbor }

Java
:  @@snip [CborSerializable.java](/examples/src/test/java/jdocs/guide/CborSerializable.java) { #guideCbor }

Configure the `CborSerializable` type to use `jackson-cbor` configuration in your `application.conf`.
We will add this configuration when Akka Persistence configuration is setup in the @ref:[Choosing a SourceProvider](source-provider.md) section of the guide.

Scala
:  @@snip [guide-shopping-cart-app.conf](/examples/src/test/resources/guide-shopping-cart-app.conf) { #guideSerializationBindingsScala }

Java
:  @@snip [guide-shopping-cart-app.conf](/examples/src/test/resources/guide-shopping-cart-app.conf) { #guideSerializationBindingsJava }

@@@ note

For Jackson serialization to work correctly in Java projects you must use the `javac` compiler parameter `-parameters` when building your project.
In @scala[sbt you can add it your sbt project by adding it to the `javacOptions` Setting: `javacOptions += "-parameters"`]@java[maven you can add an argument to `maven-compiler-plugin` plugin under `compilerArgs` ([see an example here](https://github.com/akka/akka-samples/blob/2.6/akka-sample-cqrs-java/pom.xml#L136))].

@@@

Define the persistence tags to be used in your project.
Note that partitioned tags will be used later when @ref[running the projection in Akka Cluster](running-cluster.md).
Add `ShoppingCartTags` to your project:

Scala
:  @@snip [ShoppingCartTags.scala](/examples/src/test/scala/docs/guide/ShoppingCartTags.scala) { #guideTags }

Java
:  @@snip [ShoppingCartTags.java](/examples/src/test/java/jdocs/guide/ShoppingCartTags.java) { #guideTags }

Create the `ShoppingCartApp` with an `akka.actor.typed.ActorSystem` (API: @apidoc[akka.actor.typed.ActorSystem]) for Projections to use.
Create an empty [Guardian Actor](https://doc.akka.io/docs/akka/current/typed/actor-lifecycle.html#the-guardian-actor) (the root Actor of the `ActorSystem`).
We will populate this Actor in the following steps of the guide.
Note that we are using the @scala[`docs.scaladsl`]@java[`jdocs.scaladsl`] package.
You may use any package, but we include this package in snippets throughout the guide.

Scala
:  @@snip [ShoppingCartApp.scala](/examples/src/test/scala/docs/guide/ShoppingCartApp.scala) { #guideSetup }

Java
:  @@snip [ShoppingCartApp.java](/examples/src/test/java/jdocs/guide/ShoppingCartApp.java) { #guideSetup }

