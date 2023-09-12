# GraalVM Native Image

The Local Drone Control service is configured for building a [GraalVM Native Image], for low
resource usage, faster starts, and smaller deployments.

[GraalVM Native Image]: https://www.graalvm.org/latest/reference-manual/native-image/


## Native build

To create a native image for the current build platform and architecture:

```
sbt nativeImage
```


## Docker build

To build a native image to be deployed as a Docker container:

```
docker build -f native-image/Dockerfile -t local-drone-control .
```
