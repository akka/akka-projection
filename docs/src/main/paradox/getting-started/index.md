# Getting Started Guide

By now you should understand the fundamental concepts of how a Projection works by reading about its @ref:[use cases](../use-cases.md).
This guide will briefly describe the basic components of a Projection and instruct you step-by-step on how to build a functioning application.
The example used in this guide is based on a more complete application called `akka-sample-cqrs` that's found in
@java[@extref[CQRS example project](samples:akka-samples-cqrs-java)]@scala[@extref[CQRS example project](samples:akka-samples-cqrs-scala)].

It builds a full @ref:[CQRS](../use-cases.md#command-query-responsibility-segregation-cqrs-) (Command Query Responsibility Segregation) ES (Event Sourcing) system using a combination of features from the Akka toolkit.

@@toc { depth=2 }

@@@ index

* [Setup your application](setup-your-app.md)
* [Choosing a Source Provider](source-provider.md)
* [Build a Stateful Projection handler](projection-handler.md)
* [Writing tests for a Projection](testing.md)
* [Running the Projection](running.md)
* [Monitoring](monitoring.md)
* [Manage offsets](offset-management.md)

@@@

