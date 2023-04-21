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


### Publish the events of the shopping cart

FIXME

### Consume events

FIXME

### Filters

The Service to Service eventing sample showcases filtering in two ways, one controlled on the producing side, one on the
consuming side. The combined filtering doesn't make much sense, see this as an example of two different ways to do achieve
the same thing, but placing the control over it in the producing service or the consuming service.

The shopping cart service is set up with a producer filter to only pass carts that has been tagged with the tag `medium` or `large`,
triggered by the number of items in the cart:

FIXME snippet

The analytics service is set up to not consume all shopping carts from the upstream shopping cart service but only include
carts containing 10 or more items.

FIXME snippet

## What's next?

* Turning the shopping cart highly available through Active-Active