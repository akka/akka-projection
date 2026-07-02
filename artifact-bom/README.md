Akka
====
*Akka is a powerful platform that simplifies building and operating highly responsive, resilient, and scalable services.*


The platform consists of
* the [**Akka SDK**](https://doc.akka.io/) for straightforward, rapid development with AI assist and automatic clustering. Services built with the Akka SDK are automatically clustered and can be deployed on any infrastructure.
* and [**Akka Automated Operations**](https://doc.akka.io/operations/akka-platform.html), a managed solution that handles everything for Akka SDK services from auto-elasticity to multi-region high availability running safely within your VPC.

The **Akka SDK** and **Akka Automated Operations** are built upon the foundational [**Akka libraries**](https://doc.akka.io/libraries/akka-dependencies/current/), providing the building blocks for distributed systems.


Akka Diagnostics
================

Akka Diagnostics is a suite of useful components that complement Akka libraries.

## Akka Thread Starvation Detector

The Akka Thread Starvation Detector is a diagnostic tool that monitors the dispatcher of an ActorSystem and will log a warning if the dispatcher becomes unresponsive.

## Akka Config Checker

The Akka Config Checker tries to help you by finding potential configuration issues. It is based on knowledge that the Akka Team has gathered from typical misunderstanding seen in mailing lists and customer consulting.

Reference Documentation
-----------------------

The reference documentation for all Akka libraries is available via [doc.akka.io/libraries/](https://doc.akka.io/libraries/), details for the Akka Diagnostics library
for [Scala](https://doc.akka.io/libraries/akka-diagnostics/current/?language=scala) and [Java](https://doc.akka.io/libraries/akka-diagnostics/current/?language=java).

The current versions of all Akka libraries are listed on the [Akka Dependencies](https://doc.akka.io/libraries/akka-dependencies/current/) page. Releases of the Akka Diagnostics library in this repository are listed on the [GitHub releases](https://github.com/akka/akka-diagnostics/releases) page.

## Build Token

To build locally, you need to fetch a token at https://account.akka.io/token that you have to place into `~/.sbt/1.0/akka-commercial.sbt` file like this:
```
ThisBuild / resolvers += "lightbend-akka".at("your token resolver here")
```

## Project status

This library is ready to be used in production, APIs are stable, and the Akka subscription covers support for this project.

## License

Akka is licensed under the Business Source License 1.1, please see the [Akka License FAQ](https://www.lightbend.com/akka/license-faq).

Tests and documentation are under a separate license, see the LICENSE file in each documentation and test root directory for details.
