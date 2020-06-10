# akka-projection

Akka Projection provides an abstraction for consuming a stream of `(Offset, Envelope)` tuples, which can originate from persisted events, Kafka topics, or other Alpakka connectors with this shape. It provides tools to track, restart and distribute these projections.
