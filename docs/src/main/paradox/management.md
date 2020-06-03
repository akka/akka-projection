# Management of a Projection

## Offset management

With the @apidoc[ProjectionManagement] API you can manage the offset of a projection.

To retrieve latest stored offset:

Scala
:  @@snip [CassandraProjectionDocExample.scala](/examples/src/test/scala/docs/cassandra/CassandraProjectionDocExample.scala) { #get-offset }

Java
:  @@snip [CassandraProjectionDocExample.java](/examples/src/test/java/jdocs/cassandra/CassandraProjectionDocExample.java) { #get-offset }

The offset can be cleared if the projection should be completely rebuilt, starting over again from the first offset.
The operation will automatically restart the projection.

Scala
:  @@snip [CassandraProjectionDocExample.scala](/examples/src/test/scala/docs/cassandra/CassandraProjectionDocExample.scala) { #clear-offset }

Java
:  @@snip [CassandraProjectionDocExample.java](/examples/src/test/java/jdocs/cassandra/CassandraProjectionDocExample.java) { #clear-offset }

The offset can also be updated, which can be useful if the projection is stuck with errors on a specific offset
and should skip that offset and continue with next. The operation will automatically restart the projection.

Scala
:  @@snip [CassandraProjectionDocExample.scala](/examples/src/test/scala/docs/cassandra/CassandraProjectionDocExample.scala) { #update-offset }

Java
:  @@snip [CassandraProjectionDocExample.java](/examples/src/test/java/jdocs/cassandra/CassandraProjectionDocExample.java) { #update-offset }

