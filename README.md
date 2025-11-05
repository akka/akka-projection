Akka
====
*Akka is a powerful platform that simplifies building and operating highly responsive, resilient, and scalable services.*


The platform consists of
* the [**Akka SDK**](https://doc.akka.io/java/index.html) for straightforward, rapid development with AI assist and automatic clustering. Services built with the Akka SDK are automatically clustered and can be deployed on any infrastructure.
* and [**Akka Automated Operations**](https://doc.akka.io/operations/akka-platform.html), a managed solution that handles everything for Akka SDK services from auto-elasticity to multi-region high availability running safely within your VPC.

The **Akka SDK** and **Akka Automated Operations** are built upon the foundational [**Akka libraries**](https://doc.akka.io/libraries/akka-dependencies/current/), providing the building blocks for distributed systems.



Akka Projections
================

Akka Projections provides an abstraction for consuming a stream of `Envelope` (where `Envelope` contains a payload and a trackable offset). This streams can originate from persisted events, Kafka topics, or other Alpakka connectors. 

Akka Projections also provides tools to track, restart and distribute these projections.

Reference Documentation
-----------------------

The reference documentation for all Akka libraries is available via [doc.akka.io/libraries/](https://doc.akka.io/libraries/), details for the Akka Projections libraries
for [Scala](https://doc.akka.io/libraries/akka-projection/current/?language=scala) and [Java](https://doc.akka.io/libraries/akka-projection/current/?language=java).

The current versions of all Akka libraries are listed on the [Akka Dependencies](https://doc.akka.io/libraries/akka-dependencies/current/) page. Releases of the Akka Projections libraries in this repository are listed on the [GitHub releases](https://github.com/akka/akka-projection/releases) page.

License
-------

Akka is licensed under the Business Source License 1.1, please see the [Akka License FAQ](https://akka.io/bsl-license-faq).

Tests and documentation are under a separate license, see the LICENSE file in each documentation and test root directory for details.
