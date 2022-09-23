# Management of a Projection

## Offset management

With the @apidoc[ProjectionManagement] API you can manage the offset of a projection.

To retrieve latest stored offset:

Scala
:  @@snip [CassandraProjectionDocExample.scala](/examples/src/it/scala/docs/cassandra/CassandraProjectionDocExample.scala) { #get-offset }

Java
:  @@snip [CassandraProjectionDocExample.java](/examples/src/it/java/jdocs/cassandra/CassandraProjectionDocExample.java) { #get-offset }

The offset can be cleared if the projection should be completely rebuilt, starting over again from the first offset.
The operation will automatically restart the projection.

Scala
:  @@snip [CassandraProjectionDocExample.scala](/examples/src/it/scala/docs/cassandra/CassandraProjectionDocExample.scala) { #clear-offset }

Java
:  @@snip [CassandraProjectionDocExample.java](/examples/src/it/java/jdocs/cassandra/CassandraProjectionDocExample.java) { #clear-offset }

The offset can also be updated, which can be useful if the projection is stuck with errors on a specific offset
and should skip that offset and continue with next. The operation will automatically restart the projection.

Scala
:  @@snip [CassandraProjectionDocExample.scala](/examples/src/it/scala/docs/cassandra/CassandraProjectionDocExample.scala) { #update-offset }

Java
:  @@snip [CassandraProjectionDocExample.java](/examples/src/it/java/jdocs/cassandra/CassandraProjectionDocExample.java) { #update-offset }

## Pause and resume

With the @apidoc[ProjectionManagement] API you can pause and resume processing of a projection. For example,
this can be useful when performing some data migration and projection processing cannot run while the migration
is in progress.

Scala
:  @@snip [CassandraProjectionDocExample.scala](/examples/src/it/scala/docs/cassandra/CassandraProjectionDocExample.scala) { #pause-resume }

Java
:  @@snip [CassandraProjectionDocExample.java](/examples/src/it/java/jdocs/cassandra/CassandraProjectionDocExample.java) { #pause-resume }


The paused/resumed state is stored and, and it is read when the Projections are started, for example in case of rebalance or system restart.

To retrieve the paused state:

Scala
:  @@snip [CassandraProjectionDocExample.scala](/examples/src/it/scala/docs/cassandra/CassandraProjectionDocExample.scala) { #is-paused }

Java
:  @@snip [CassandraProjectionDocExample.java](/examples/src/it/java/jdocs/cassandra/CassandraProjectionDocExample.java) { #is-paused }

## Status tracking

The status of a `Projection` can be tracked by implementing a @apidoc[StatusObserver] and enable it with 
`withStatusObserver` before running the `Projection`.

The `StatusObserver` is called when errors occur and envelopes are retried or the projection failed (restarted).
It also has callbacks for processing progress and projection lifecyle.

The intention is that the implementation of the `StatusObserver` would maintain a view that can be accessed
from an administrative UI to have an overview of current status of the projections. 
