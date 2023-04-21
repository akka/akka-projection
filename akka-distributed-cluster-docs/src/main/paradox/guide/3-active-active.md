# Part 3: Active-active

For an Active-Active shopping cart, the same service will run in different data centers or cloud regions - each called a replica.

## Turning the shopping cart into a Replicated Entity

FIXME show off RES API

## Filters

By default, events from all Replicated Event Sourced entities are replicated.

The same kind of filters as described for @ref:[Service to Service](2-service-to-service.md#filters) can be used for
Replicated Event Sourcing.

The producer defined filter:

Scala
:  @@snip [ShoppingCart.scala](/samples/replicated/shopping-cart-service-scala/src/main/scala/shopping/cart/ShoppingCart.scala) { #init }

Java
:  @@snip [ShoppingCart.java](/samples/replicated/shopping-cart-service-java/src/main/java/shopping/cart/ShoppingCart.java) { #init }

Consumer defined filters are updated as described in @extref:[Akka Projection gRPC Consumer defined filter](akka-projection:grpc.md#consumer-defined-filter)

One thing to note is that `streamId` is always the same as the `entityType` when using Replicated Event Sourcing.

The entity id based filter criteria must include the replica id as suffix to the entity id, with `|` separator.

Replicated Event Sourcing is bidirectional replication, and therefore you would typically have to define the same
filters on both sides. That is not handled automatically.

## What's next?

* Configuring and deploying the service with Kubernetes