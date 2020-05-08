# Events from Akka Persistence

## Dependencies

To use the Event Sourced module of Akka Projections add the following dependency in your project:

@@dependency [sbt,Maven,Gradle] {
  group=com.typesafe.akka
  artifact=akka-projection-eventsourced_$scala.binary.version$
  version=$project.version$
}

Akka Projections require Akka $akka.version$ or later, see @ref:[Akka version](overview.md#akka-version).

@@project-info{ projectId="akka-projection-eventsourced" }

### Transitive dependencies

The table below shows `akka-projection-eventsourced`'s direct dependencies and the second tab shows all libraries it depends on transitively.

@@dependencies{ projectId="akka-projection-eventsourced" }

