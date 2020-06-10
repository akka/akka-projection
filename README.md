# akka-projection

Akka Projection provides an abstraction for consuming a stream of `Envelope` (where `Envelope` contains a payload and a trackable offset). This streams can originate from persisted events, Kafka topics, or other Alpakka connectors. 

Akka Projection also provides tools to track, restart and distribute these projections.
