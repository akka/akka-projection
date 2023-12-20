# Error handling

Errors are handled at two levels.

## Handler recovery

A @apidoc[HandlerRecoveryStrategy] can be defined for the `Projection` to specify what to do when processing
of an envelope fails. The alternatives are:

* `fail` - If the first attempt to invoke the handler fails it will immediately give up and the projection will be
  @ref:[restarted](#projection-restart).
* `skip` - If the first attempt to invoke the handler fails it will immediately give up, discard the element and
  continue with next.
* `retryAndFail(retries, delay)`  - If the first attempt to invoke the handler fails it will retry
  invoking the handler with the same envelope a number of `retries` with the `delay` between each attempt.
  It will give up and @ref:[restart](#projection-restart) the projection if all attempts fail.
* `retryAndSkip(retries, delay)`  - If the first attempt to invoke the handler fails it will retry
  invoking the handler with the same envelope a number of `retries` with the `delay` between each attempt.
  It will give up, discard the element and continue with next if all attempts fail.

The following example is using the `CassandraProjection` but the same can be used with any other
@ref:[Projection type](overview.md).

The `HandlerRecoveryStrategy` can be defined `withRecoveryStrategy`:

Scala
:  @@snip [CassandraProjectionDocExample.scala](/examples/src/test/scala/docs/cassandra/CassandraProjectionDocExample.scala) { #withRecoveryStrategy }

Java
:  @@snip [CassandraProjectionDocExample.java](/examples/src/test/java/jdocs/cassandra/CassandraProjectionDocExample.java) { #withRecoveryStrategy }

If the recovery strategy is not defined on the `Projection` the default is `fail`, and that can be defined
in configuration:

@@snip [reference.conf](/akka-projection-core/src/main/resources/reference.conf) { #recovery-strategy }
 

## Projection restart

The `Projection` will be restarted if it fails, for example if the offset can't be saved or processing of an
envelope fails after applying the @ref:[Handler recovery](#handler-recovery). Restart means that the projection
is restarted from latest saved offset. Projections are restarted in case of failures by default, but it can be
customized.

The following example is using the `CassandraProjection` but the same can be used with any other
@ref:[Projection type](overview.md).

The restart can be defined with exponential backoff settings:

Scala
:  @@snip [CassandraProjectionDocExample.scala](/examples/src/test/scala/docs/cassandra/CassandraProjectionDocExample.scala) { #withRestartBackoff }

Java
:  @@snip [CassandraProjectionDocExample.java](/examples/src/test/java/jdocs/cassandra/CassandraProjectionDocExample.java) { #withRestartBackoff }
 
If the recovery strategy is not defined on the `Projection` the default is defined in configuration:

@@snip [reference.conf](/akka-projection-core/src/main/resources/reference.conf) { #restart-backoff }
