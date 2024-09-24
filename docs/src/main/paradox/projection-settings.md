# Projection Settings

A Projection is a background process that continuously consume event envelopes from a `Source`. Therefore, in case of failures, it is automatically restarted. This is done by automatically wrapping the `Source` with a [RestartSource with backoff on failures](https://doc.akka.io/libraries/akka-core/current/stream/operators/RestartSource/onFailuresWithBackoff.html#restartsource-onfailureswithbackoff).

By default, the backoff configuration defined in the reference configuration is used. Those values can be overriden in the `application.conf` file or programatically as shown below.

Scala
:  @@snip [CassandraProjectionDocExample.scala](/examples/src/test/scala/docs/cassandra/CassandraProjectionDocExample.scala) { #projection-imports #projection-settings }

Java
:  @@snip [CassandraProjectionDocExample.java](/examples/src/test/java/jdocs/cassandra/CassandraProjectionDocExample.java) { #projection-imports  #projection-settings }

## Configuration

@@snip [reference.conf](/akka-projection-core/src/main/resources/reference.conf)
