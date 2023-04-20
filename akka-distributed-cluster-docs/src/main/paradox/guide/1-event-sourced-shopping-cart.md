# Part 1: Event Sourced Shopping Cart

As the other features of Akka Distributed Cluster build on top of Event Sourcing, let us start by implementing a shopping
cart using the [Akka Event Sourced Behavior API](akka:typed/persistence.html).

## What is Event Sourcing

FIXME

## Implementing an Event Sourced actor

FIXME sample snippets for cart as just an ESB

## Running the actor in an Akka cluster

FIXME set up sharding

## Allowing access from the outside with Akka gRPC

FIXME

## What's next?

 * Making the events of the service available for consumption in a separately deployed service