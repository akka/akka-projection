# GraalVM Native Image

The Local Drone Control service is configured for building a [GraalVM Native Image], for low
resource usage, faster starts, and smaller deployments.

[GraalVM Native Image]: https://www.graalvm.org/latest/reference-manual/native-image/


## Native build for single-node service

Note: to build locally with the `native-maven-plugin` you need to first [set up GraalVM].

[set up GraalVM]: https://www.graalvm.org/latest/docs/getting-started/

To create a native image for the current build platform and architecture:

```
mvn -DskipTests=true -Pnative package
```


## Native build for multi-node service

To create a native image to run as a multi-node Akka Cluster with PostgreSQL:

```
mvn -DskipTests=true -Pnative -Pclustered package
```


## Docker build  for single-node service

To build a native image, within Docker, to be deployed as a Docker container:

```
docker build -f native-image/Dockerfile -t local-drone-control .
```


## Docker build for multi-node service

To build a native image to be deployed as a Docker container for a multi-node Akka Cluster with PostgreSQL:

```
docker build -f native-image/Dockerfile --build-arg profile=clustered -t local-drone-control .
```
