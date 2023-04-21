# Part 2: Service to Service eventing

To implement Service to Service eventing, we will use two services, the shopping cart defined in the previous step and 
a downstream analytics service. 

Each of the services have their own lifecycle and are deployed separately, possibly in different data centers or
cloud regions.

## gRPC transport for consuming events

Akka Projection allows for creating read side views, or projections, that are eventually consistent representations
of the events for an entity. Such views have historically been possible to define in the service that owns the entity,
for an example of this, you can look at the [popularity projection in the Akka Microservice Guide](https://developer.lightbend.com/docs/akka-guide/microservices-tutorial/projection-query.html).

Service to Service defines a gRPC service in the service where the entity lives and that makes the events available for
other services to consume with an effectively once delivery guarantee without requiring a message broker in between services.

FIXME graphic that is more overview and less step by step? (this is the same as projection gRPC)

![overview.png](images/service-to-service-overview.png)

1. An Entity stores events in its journal in service A.
1. Consumer in service B starts an Akka Projection which locally reads its offset for service A's replication stream.
1. Service B establishes a replication stream from service A.
1. Events are read from the journal.
1. Event is emitted to the replication stream.
1. Event is handled.
1. Offset is stored.
1. Producer continues to read new events from the journal and emit to the stream. As an optimization, events can also be published directly from the entity to the producer.


## Publish the events of the shopping cart

The cart itself does not need any changes for publishing persisted events, but we need to configure and bind a producer
service for it to allow other services to consume the events.


Scala
:  @@snip [PublishEvents.scala](/samples/grpc/shopping-cart-service-scala/src/main/scala/shopping/cart/PublishEvents.scala) { #eventProducerService }

Java
:  @@snip [PublishEvents.java](/samples/grpc/shopping-cart-service-java/src/main/java/shopping/cart/PublishEvents.java) { #eventProducerService }

Events can be transformed by application specific code on the producer side. The purpose is to be able to have a
different public representation from the internal representation (stored in journal). The transformation functions
are registered when creating the `EventProducer` service. Here is an example of one of those transformation functions:

Scala
:  @@snip [PublishEvents.scala](/samples/grpc/shopping-cart-service-scala/src/main/scala/shopping/cart/PublishEvents.scala) { #transformItemAdded }

Java
:  @@snip [PublishEvents.java](/samples/grpc/shopping-cart-service-java/src/main/java/shopping/cart/PublishEvents.java) { #transformItemAdded }

To omit an event the transformation function can return @scala[`None`]@java[`Optional.empty()`].

That `EventProducer` service is started in an Akka gRPC server like this:

Scala
:  @@snip [ShoppingCartServer.scala](/samples/grpc/shopping-cart-service-scala/src/main/scala/shopping/cart/ShoppingCartServer.scala) { #startServer }

Java
:  @@snip [ShoppingCartServer.java](/samples/grpc/shopping-cart-service-java/src/main/java/shopping/cart/ShoppingCartServer.java) { #startServer }


## Consume events

FIXME more about the consumer project

The configuration for the `GrpcReadJournal` may look like this:

@@snip [grpc.conf](/samples/grpc/shopping-analytics-service-java/src/main/resources/grpc.conf) { }

The `client` section in the configuration defines where the producer is running. It is an @extref:[Akka gRPC configuration](akka-grpc:client/configuration.html#by-configuration) with several connection options.


## Filters

The Service to Service eventing sample showcases filtering in two ways, one controlled on the producing side, one on the
consuming side. The combined filtering doesn't make much sense, see this as an example of two different ways to do achieve
the same thing, but placing the control over it in the producing service or the consuming service.

The shopping cart service is set up with a producer filter to only pass carts that has been tagged with the tag `medium` or `large`,
triggered by the number of items in the cart:

FIXME snippet

The analytics service is set up to not consume all shopping carts from the upstream shopping cart service but only include
carts containing 10 or more items.

FIXME snippet

## Complete Sample Projects

The complete sample can be downloaded from github, the shopping cart:

* Java: https://github.com/akka/akka-projection/tree/main/samples/grpc/shopping-cart-service-java
* Scala: https://github.com/akka/akka-projection/tree/main/samples/grpc/shopping-cart-service-scala

And the consuming analytics service:

* Java: https://github.com/akka/akka-projection/tree/main/samples/grpc/shopping-analytics-service-java
* Scala: https://github.com/akka/akka-projection/tree/main/samples/grpc/shopping-analytics-service-scala

FIXME running locally instructions here as well

## What's next?

* Turning the shopping cart highly available through Active-Active