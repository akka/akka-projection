---
project.description: Snapshot builds of Akka Projection are provided via the Sonatype snapshot repository.
---
# Snapshots

[snapshots-badge]:  https://img.shields.io/nexus/s/com.lightbend.akka/akka-projection-core_2.13?server=https%3A%2F%2Foss.sonatype.org
[snapshots]:        https://oss.sonatype.org/content/repositories/snapshots/com/lightbend/akka/akka-projection-core_2.13/

Snapshots are published to the Sonatype Snapshot repository after every successful build on main.
Add the following to your project build definition to resolve Akka Projection's snapshots:

## Configure repository

Maven
:   ```xml
    <project>
    ...
      <repositories>
        <repository>
            <id>snapshots-repo</id>
            <name>Sonatype snapshots</name>
            <url>https://oss.sonatype.org/content/repositories/snapshots</url>
        </repository>
      </repositories>
    ...
    </project>
    ```

sbt
:   ```scala
    resolvers += Resolver.sonatypeRepo("snapshots")
    ```

Gradle
:   ```gradle
    repositories {
      maven {
        url  "https://oss.sonatype.org/content/repositories/snapshots"
      }
    }
    ```

## Documentation

The [snapshot documentation](https://doc.akka.io/docs/akka-projection/snapshot) is updated with every snapshot build.

## Versions

Latest published snapshot version is [![snapshots-badge][]][snapshots]

The snapshot repository is cleaned from time to time with no further notice. Check [Sonatype snapshots Akka Projection files](https://oss.sonatype.org/content/repositories/snapshots/com/lightbend/akka/) to see what versions are currently available.
