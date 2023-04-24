# Part 1: Event Sourced Shopping Cart

As the other features of Akka Distributed Cluster are build on top of Event Sourcing, let us start by implementing a shopping
cart using the [Akka Event Sourced Behavior API](akka:typed/persistence.html). When this first step is completed, end users 
will be able to add and remove items to a cart and finally check it out.

We will build the cart as an Event Sourced entity, if you are unfamiliar with Event Sourcing, refer to the
@extref[Event Sourcing section in the Akka guide](akka-guide:concepts/event-sourcing.html) for an explanation. 
The [Event Sourcing with Akka video](https://akka.io/blog/news/2020/01/07/akka-event-sourcing-video) is also a good starting point for learning Event Sourcing.

## Implementing an Event Sourced shopping cart

### Commands

Commands are the "external" API of an entity. Entity state can only be changed by commands. The results of commands are emitted as events. A command can request state changes, but different events might be generated depending on the current state of the entity. A command can also be rejected if it has invalid input or can’t be handled by current state of the entity.

Scala
:  @@snip [ShoppingCart.scala](/samples/grpc/shopping-cart-service-scala/src/main/scala/shopping/cart/ShoppingCart.scala) { #commands #events }

Java
:  @@snip [ShoppingCart.java](/samples/grpc/shopping-cart-service-java/src/main/java/shopping/cart/ShoppingCart.java) { #commands #events }

### State

Items added to the Cart are added to a `Map`. The contents of the `Map` comprise the Cart’s state along with a customer id and customer category for the customer
owning the cart, if set, and a checkout timestamp if the cart was checked out:

Scala
:  @@snip [ShoppingCart.scala](/samples/grpc/shopping-cart-service-scala/src/main/scala/shopping/cart/ShoppingCart.scala) { #state }

Java
:  @@snip [ShoppingCart.java](/samples/grpc/shopping-cart-service-java/src/main/java/shopping/cart/ShoppingCart.java) { #state }


### Command handler

The Cart entity will receive commands that request changes to Cart state. We will implement a command handler to process these commands and emit a reply. Our business logic allows only items to be added which are not in the cart yet and require a positive quantity.


Scala
:  @@snip [ShoppingCart.scala](/samples/grpc/shopping-cart-service-scala/src/main/scala/shopping/cart/ShoppingCart.scala) { #commandHandler }

Java
:  @@snip [ShoppingCart.java](/samples/grpc/shopping-cart-service-java/src/main/java/shopping/cart/ShoppingCart.java) { #commandHandler }


### Event handler

From commands, the entity creates events that represent state changes. Aligning with the command handler above, the entity’s event handler reacts to events and updates the state. The events are continuously persisted to the Event Journal datastore, while the entity state is kept in memory. Other parts of the application may listen to the events. In case of a restart, the entity recovers its latest state by replaying the events from the Event Journal.


Scala
:  @@snip [ShoppingCart.scala](/samples/grpc/shopping-cart-service-scala/src/main/scala/shopping/cart/ShoppingCart.scala) { #eventHandler }

Java
:  @@snip [ShoppingCart.java](/samples/grpc/shopping-cart-service-java/src/main/java/shopping/cart/ShoppingCart.java) { #eventHandler }


### Wiring it all together

To glue the command handler, event handler, and state together, we need some initialization code. Our code will distribute the Cart entities over nodes in the Akka Cluster with @extref[Cluster Sharding](akka:typed/cluster-sharding.html), enable snapshots to reduce recovery time when the entity is started, and restart with backoff in the case of failure.

Scala
:  @@snip [ShoppingCart.scala](/samples/grpc/shopping-cart-service-scala/src/main/scala/shopping/cart/ShoppingCart.scala) { #init }

Java
:  @@snip [ShoppingCart.java](/samples/grpc/shopping-cart-service-java/src/main/java/shopping/cart/ShoppingCart.java) { #init }


## Serialization

The state, commands and events of the entity must be serializable because they are written to the datastore or sent between nodes within the Akka cluster. The template project includes built-in CBOR serialization using the @extref[Akka Serialization Jackson module](akka:serialization-jackson.html). This section describes how serialization is implemented. You do not need to do anything specific to take advantage of CBOR, but this section explains how it is included.
The state, commands and events are marked as CborSerializable which is configured to use the built-in CBOR serialization. The template project includes this marker interface CborSerializable:

Scala
:  @@snip [CborSerializable.scala](/samples/grpc/shopping-cart-service-scala/src/main/scala/shopping/cart/CborSerializable.scala) { }

Java
:  @@snip [CborSerializable.java](/samples/grpc/shopping-cart-service-java/src/main/java/shopping/cart/CborSerializable.java) { }

Configuration in the application configuration to select the serializer:

Scala
:  @@snip [application.conf](/samples/grpc/shopping-cart-service-scala/src/main/resources/serialization.conf) { }

Java
:  @@snip [application.conf](/samples/grpc/shopping-cart-service-java/src/main/resources/serialization.conf) { }

## Client access with Akka gRPC

To allow users to actually use the service we need a public API reachable over the internet. For this we will use @extref[Akka gRPC](akka-grpc:)
giving us a type safe, efficient protocol that allows clients to be written in many languages.

The service descriptor for the API is defined in protobuf and mirrors the set of commands the entity accepts:

Scala
:  @@snip [ShoppingCartService.proto](/samples/grpc/shopping-cart-service-scala/src/main/protobuf/ShoppingCartService.proto) { }

Java
:  @@snip [ShoppingCartService.proto](/samples/grpc/shopping-cart-service-java/src/main/protobuf/ShoppingCartService.proto) { }

When compiling the project the Akka gRPC @scala[sbt]@java[maven] plugin generates a service interface for us to implement.
Our implementation of it interacts with the entity:

Scala
:  @@snip [ShoppingCartServiceImpl.scala](/samples/grpc/shopping-cart-service-scala/src/main/scala/shopping/cart/ShoppingCartServiceImpl.scala) {}

Java
:  @@snip [ShoppingCartServiceImpl.java](/samples/grpc/shopping-cart-service-java/src/main/java/shopping/cart/ShoppingCartServiceImpl.java) {}

Finally, we need to start the HTTP server, making service implementation available for calls from external clients:

Scala
:  @@snip [ShoppingCartServer.scala](/samples/grpc/shopping-cart-service-scala/src/main/scala/shopping/cart/ShoppingCartServer.scala) { #startServerNoPublish }

Java
:  @@snip [ShoppingCartServer.java](/samples/grpc/shopping-cart-service-java/src/main/java/shopping/cart/ShoppingCartServer.java) { #startServerNoPublish }

The Akka HTTP server must be running with HTTP/2 to serve gRPC, this is done through config:

Scala
:  @@snip [ShoppingCartServer.scala](/samples/grpc/shopping-cart-service-scala/src/main/resources/grpc.conf) { #http2 }

Java
:  @@snip [ShoppingCartServer.java](/samples/grpc/shopping-cart-service-java/src/main/resources/grpc.conf) { #http2 }


## Running the sample

The complete sample can be downloaded from github, but note that it also includes the next step of the guide:

  * Java: https://github.com/akka/akka-projection/tree/main/samples/grpc/shopping-cart-service-java
  * Scala: https://github.com/akka/akka-projection/tree/main/samples/grpc/shopping-cart-service-scala

Before running the sample locally you will need to run a PostgreSQL instance in docker, it can be started with the included
`docker-compose.yml`. Run it and create the needed database schema:

```shell
docker-compose up -d
docker exec -i postgres_db psql -U postgres -t < ddl-scripts/create_tables.sql
```

@@@ div { .group-scala }

To start the sample:

```shell
sbt -Dconfig.resource=local1.conf run
```

And optionally one or two more Akka cluster nodes:

```shell
sbt -Dconfig.resource=local2.conf run
sbt -Dconfig.resource=local3.conf run
```

@@@

@@@ div { .group-java }

```shell
mvn compile exec:exec -DAPP_CONFIG=local1.conf
```

And optionally one or two more Akka cluster nodes:
```shell
mvn compile exec:exec -DAPP_CONFIG=local2.conf
mvn compile exec:exec -DAPP_CONFIG=local3.conf
```

@@@

Try it with [grpcurl](https://github.com/fullstorydev/grpcurl):

```shell
# add item to cart
grpcurl -d '{"cartId":"cart1", "itemId":"socks", "quantity":3}' -plaintext 127.0.0.1:8101 shoppingcart.ShoppingCartService.AddItem

# get cart
grpcurl -d '{"cartId":"cart1"}' -plaintext 127.0.0.1:8101 shoppingcart.ShoppingCartService.GetCart

# update quantity of item
grpcurl -d '{"cartId":"cart1", "itemId":"socks", "quantity":5}' -plaintext 127.0.0.1:8101 shoppingcart.ShoppingCartService.UpdateItem

# check out cart
grpcurl -d '{"cartId":"cart1"}' -plaintext 127.0.0.1:8101 shoppingcart.ShoppingCartService.Checkout
```

or same `grpcurl` commands to port 8102 to reach node 2.

## What's next?

 * Making the events of the service available for consumption in a separately deployed service