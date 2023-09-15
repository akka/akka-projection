# GraalVM Native Image

The Local Drone Control service is configured for building a [GraalVM Native Image], for low
resource usage, faster starts, and smaller deployments.

[GraalVM Native Image]: https://www.graalvm.org/latest/reference-manual/native-image/


## Native build

Note: to build locally with the `native-maven-plugin` you need to first [set up GraalVM].

[set up GraalVM]: https://www.graalvm.org/latest/docs/getting-started/

To create a native image for the current build platform and architecture:

```
mvn -DskipTests=true -Pnative compile package
```


## Docker build

To build a native image, within Docker, to be deployed as a Docker container:

```
docker build -f native-image/Dockerfile -t local-drone-control .
```
