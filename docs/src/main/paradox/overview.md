# Overview

.

## Project Info

@@project-info{ projectId="core" }

## Dependencies

This plugin requires **Akka $akka.version$** or later. See [Akka's Binary Compatibility Rules](https://doc.akka.io/docs/akka/current/common/binary-compatibility-rules.html) for details.

@@dependency [sbt,Maven,Gradle] {
  group=com.typesafe.akka
  artifact=akka-projection_$scala.binary.version$
  version=$project.version$
  symbol=AkkaVersion
  value=$akka.version$
  group1=com.typesafe.akka
  artifact1=akka-stream_$scala.binary.version$
  version1=AkkaVersion
}

Note that it is important that all `akka-*` dependencies are in the same version, so it is recommended to depend on them explicitly to avoid problems with transient dependencies causing an unlucky mix of versions.

The table below shows Akka Projection's direct dependencies and the second tab shows all libraries it depends on transitively.

@@dependencies{ projectId="core" }

## Contributing

Please feel free to contribute to Akka and Akka Projection by reporting issues you identify, or by suggesting changes to the code. Please refer to our [contributing instructions](https://github.com/akka/akka/blob/master/CONTRIBUTING.md) to learn how it can be done.

We want Akka to strive in a welcoming and open atmosphere and expect all contributors to respect our [code of conduct](https://www.lightbend.com/conduct).
