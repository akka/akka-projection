# Running the Projection in Akka Cluster

Running the Projection with [Akka Cluster](https://doc.akka.io/libraries/akka-core/current/typed/cluster.html) allows us to add two important aspects to our system: availability and scalability.
A Projection running as a single Actor creates a single point of failure (availability), when the app shuts down for any reason, the projection is no longer running until it's started again.
A Projection running as a single Actor creates a processing bottleneck (scalability), all messages from the @apidoc[SourceProvider] are processed by a single Actor on a single machine.
By using a [Sharded Daemon Process](https://doc.akka.io/libraries/akka-core/current/typed/cluster-sharded-daemon-process.html#sharded-daemon-process) with Akka Cluster and [Akka Cluster Sharding](https://doc.akka.io/libraries/akka-core/current/typed/cluster-sharding.html) we can scale up the Projection and make it more available by running at least as many instances of the same Projection as we have cluster members.
As Akka cluster members join and leave the cluster the Sharded Daemon Process will automatically scale and rebalance Sharded Daemon Processes (Projection instances) accordingly.

Running the Projection as a Sharded Daemon Process requires no changes to our projection handler and repository, we only need to change the way in which the actor that runs the Projection is initialized.
In the cluster version of this app we use a different configuration that configures Akka cluster.
The main difference in the app itself is that we use @apidoc[ShardedDaemonProcess] to initialize the Projection actor on our behalf.
Instead of creating single instances of our repository and projection handler we create factory methods that encapsulate their instantiation along with the sharded daemon actors (1 per tag) assigned to this cluster member.

Add a new configuration file `guide-shopping-cart-cluster-app.conf` to your `src/main/resources/` directory.
This configuration is largely the same as before, but includes extra configuration to enable cluster connectivity and sharding:

@@snip [guide-shopping-cart-cluster-app.conf](/examples/src/test/resources/guide-shopping-cart-cluster-app.conf) { #guideClusterConfig }

Add the `ShoppingCartClusterApp` to your project:

Scala
:  @@snip [ShoppingCartClusterApp.scala](/examples/src/test/scala/docs/guide/ShoppingCartClusterApp.scala) { #guideClusterSetup }

Java
:  @@snip [ShoppingCartClusterApp.java](/examples/src/test/java/jdocs/guide/ShoppingCartClusterApp.java) { #guideClusterSetup }
    
Before running the app we must first run the `EventGeneratorApp` in `cluster` mode in order to generate new shopping cart events for multiple tags, instead of just one.
Shopping cart events are tagged in a similar way to the sharded entities themselves.
Given a sequence of tags from `0..n` a hash is generated using the sharding entity key, the shopping cart id.
The hash is modded `%` by the number of tags in the sequence to choose a tag from the sequence.
See the @ref:[Tagging Events in EventSourcedBehavior](../running.md#tagging-events-in-eventsourcedbehavior) section of the documentation for an example of how events can be tagged with Akka Persistence.

The same `EventGeneratorApp` from the previous @ref:[Running the Projection](running.md) section can be used to generate events for this app with an additional argument `cluster`.
Run the app:

<!-- run from repo:
sbt "examples/test:runMain docs.guide.EventGeneratorApp cluster"
sbt "examples/test:runMain jdocs.guide.EventGeneratorApp cluster"
-->

sbt
:   @@@vars
```
sbt "runMain docs.guide.EventGeneratorApp cluster"
```
@@@

Maven
:   @@@vars
```
mvn compile exec:java -Dexec.mainClass="jdocs.guide.EventGeneratorApp" -Dexec.args="cluster"
```
@@@

When the app is running you will observe that the logs show events written to different tags (`carts-0`, `carts-1`, etc.), instead of just one (`shopping-cart`).

```
[2020-08-13 15:18:58,383] [INFO] [docs.guide.EventGeneratorApp$] [] [EventGenerator-akka.actor.default-dispatcher-19] - id [6059e] tag [carts-1] event: ItemQuantityAdjusted(6059e,cat t-shirt,1,2) MDC: {persistencePhase=persist-evt, akkaAddress=akka://EventGenerator@127.0.1.1:25520, akkaSource=akka://EventGenerator/system/sharding/shopping-cart-event/903/6059e, sourceActorSystem=EventGenerator, persistenceId=6059e}
```

Run the first member of your new Akka cluster:

<!-- run from repo:
sbt "examples/test:runMain docs.guide.ShoppingCartClusterApp 2551"
sbt "examples/test:runMain jdocs.guide.ShoppingCartClusterApp 2551"
-->

sbt
:   @@@vars
```
sbt "runMain docs.guide.ShoppingCartClusterApp 2551
```
@@@

Maven
:   @@@vars
```
mvn compile exec:java -Dexec.mainClass="jdocs.guide.ShoppingCartClusterApp" -Dexec.args="2551"
```
@@@

When the app is running you will observe that it will process all the shopping cart event tags, because it's the only member of the cluster.

```
[2020-08-13 15:03:39,809] [INFO] [docs.guide.ItemPopularityProjectionHandler] [] [ShoppingCartClusterApp-akka.actor.default-dispatcher-43] - ItemPopularityProjectionHandler(carts-1) item popularity for 'akka t-shirt': [1080] MDC: {}   
[2020-08-13 15:03:39,810] [INFO] [docs.guide.ItemPopularityProjectionHandler] [] [ShoppingCartClusterApp-akka.actor.default-dispatcher-40] - ItemPopularityProjectionHandler(carts-2) item popularity for 'bowling shoes': [1241] MDC: {}  
[2020-08-13 15:03:39,812] [INFO] [docs.guide.ItemPopularityProjectionHandler] [] [ShoppingCartClusterApp-akka.actor.default-dispatcher-43] - ItemPopularityProjectionHandler(carts-0) item popularity for 'akka t-shirt': [1080] MDC: {}
...
```

Run a second member to expand the Akka cluster member count to 2.

<!-- run from repo:
sbt "examples/test:runMain docs.guide.ShoppingCartClusterApp 2552"
sbt "examples/test:runMain jdocs.guide.ShoppingCartClusterApp 2552"
-->

sbt
:   @@@vars
```
sbt "runMain docs.guide.ShoppingCartClusterApp 2552
```
@@@

Maven
:   @@@vars
```
mvn compile exec:java -Dexec.mainClass="jdocs.guide.ShoppingCartClusterApp" -Dexec.args="2552"
```
@@@

When the second app is running you will observe a sharding rebalance complete and then see a distinct set of tagged events processed on each cluster member.

A rebalance occurs and tag `carts-0` is assigned to the new cluster member. 
Only tags `carts-1` and `carts-2` are processed by the first member.

```
[2020-08-13 15:03:59,019] [INFO] [akka.cluster.sharding.DDataShardCoordinator] [] [ShoppingCartClusterApp-akka.actor.default-dispatcher-41] - Starting rebalance for shards [0]. Current shards rebalancing: [] MDC: {akkaAddress=akka://ShoppingCa
rtClusterApp@127.0.0.1:2551, sourceThread=ShoppingCartClusterApp-akka.actor.default-dispatcher-44, akkaSource=akka://ShoppingCartClusterApp@127.0.0.1:2551/system/sharding/sharded-daemon-process-shopping-cartsCoordinator/singleton/coordinator, 
sourceActorSystem=ShoppingCartClusterApp, akkaTimestamp=19:03:59.019UTC}                                                                                                                                                                           
[2020-08-13 15:04:35,261] [INFO] [docs.guide.ItemPopularityProjectionHandler] [] [ShoppingCartClusterApp-akka.actor.default-dispatcher-43] - ItemPopularityProjectionHandler(carts-1) item popularity for 'skis': [1244] MDC: {}           
[2020-08-13 15:04:36,802] [INFO] [docs.guide.ItemPopularityProjectionHandler] [] [ShoppingCartClusterApp-akka.actor.default-dispatcher-40] - ItemPopularityProjectionHandler(carts-2) item popularity for 'skis': [1246] MDC: {}           
[2020-08-13 15:04:36,805] [INFO] [docs.guide.ItemPopularityProjectionHandler] [] [ShoppingCartClusterApp-akka.actor.default-dispatcher-40] - ItemPopularityProjectionHandler(carts-2) item popularity for 'akka t-shirt': [1136] MDC: {}   
[2020-08-13 15:04:36,807] [INFO] [docs.guide.ItemPopularityProjectionHandler] [] [ShoppingCartClusterApp-akka.actor.default-dispatcher-43] - ItemPopularityProjectionHandler(carts-2) item popularity for 'skis': [1249] MDC: {}           
[2020-08-13 15:04:39,262] [INFO] [docs.guide.ItemPopularityProjectionHandler] [] [ShoppingCartClusterApp-akka.actor.default-dispatcher-41] - ItemPopularityProjectionHandler(carts-1) item popularity for 'cat t-shirt': [1239] MDC: {}                  
...
```

When the second member joins the cluster it is assigned tag `carts-0` and begins processing events beginning from the last successfully processed offset.

```
[2020-08-13 15:04:02,692] [INFO] [docs.guide.ItemPopularityProjectionHandler] [] [ShoppingCartClusterApp-akka.actor.default-dispatcher-5] - ItemPopularityProjectionHandler(carts-0) item popularity for 'bowling shoes': [1275] MDC: {}   
[2020-08-13 15:04:02,695] [INFO] [docs.guide.ItemPopularityProjectionHandler] [] [ShoppingCartClusterApp-akka.actor.default-dispatcher-40] - ItemPopularityProjectionHandler(carts-0) item popularity for 'akka t-shirt': [1110] MDC: {}   
[2020-08-13 15:04:02,699] [INFO] [docs.guide.ItemPopularityProjectionHandler] [] [ShoppingCartClusterApp-akka.actor.default-dispatcher-40] - ItemPopularityProjectionHandler(carts-0) item popularity for 'cat t-shirt': [1203] MDC: {}
...
```
