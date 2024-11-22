# Akka Projections

The Akka family of projects is managed by teams at [Akka](https://akka.io/) with help from the community.

Akka Projections provides an abstraction for consuming a stream of `Envelope` (where `Envelope` contains a payload and a trackable offset). This streams can originate from persisted events, Kafka topics, or other Alpakka connectors. 

Akka Projections also provides tools to track, restart and distribute these projections.

License
-------

Akka is licensed under the Business Source License 1.1, please see the [Akka License FAQ](https://akka.io/bsl-license-faq).

Tests and documentation are under a separate license, see the LICENSE file in each documentation and test root directory for details.
