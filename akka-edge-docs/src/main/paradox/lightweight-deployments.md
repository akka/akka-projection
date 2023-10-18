# Lightweight deployments

Deployments at the edge of the cloud may need to minimize resource usage and be capable of running in
resource-constrained environments. Akka Edge applications can be configured and built to run with low resource usage
and adapt to changing resource needs.

Some approaches to running lightweight deployments for Akka Edge applications include:

* @ref[Using lightweight Kubernetes distributions](#lightweight-kubernetes)
* @ref[Using cloud-optimized JVMs](#cloud-optimized-jvms)
* @ref[Building GraalVM Native Image executables](#graalvm-native-image)
* @ref[Configuring adaptive resource usage with multidimensional autoscaling](#multidimensional-autoscaling)

These approaches are useful when running applications in edge environments that are on-premise or in 5G edge computing
infrastructure, including cloud provider products such as [AWS Wavelength], [AWS Outposts], [Google Distributed Cloud
Edge], or [Azure Stack Edge].

[AWS Wavelength]: https://aws.amazon.com/wavelength/
[AWS Outposts]: https://aws.amazon.com/outposts/
[Google Distributed Cloud Edge]: https://cloud.google.com/distributed-cloud-edge
[Azure Stack Edge]: https://azure.microsoft.com/en-us/products/azure-stack/edge/


## Lightweight Kubernetes

Kubernetes has become the standard orchestration tool for deploying containers and there are lightweight Kubernetes
distributions that are specifically designed for edge computing environments. [K3s] and [MicroK8s] are lightweight
Kubernetes distributions that are suitable for deploying containerized Akka Edge applications.

[K3s]: https://k3s.io
[MicroK8s]: https://microk8s.io


## Cloud-optimized JVMs

The Java Virtual Machine (JVM) can be configured to run with lower resource usage. OpenJDK's [Project Leyden] is not
released yet, but is looking to improve the startup time, time to peak performance, and the footprint of Java programs.

[Project Leyden]: https://openjdk.org/projects/leyden/

### OpenJ9

[OpenJ9] is a JVM that is optimized for running in cloud environments and configured for lower resource usage. Options
include [tuning for virtualized environments][openj9-virtualized], [class data sharing][openj9-shrc], and
[ahead-of-time (AOT) compilation][openj9-aot] for faster starts and warmup.

[OpenJ9]: https://eclipse.dev/openj9/
[openj9-virtualized]: https://eclipse.dev/openj9/docs/xtunevirtualized/
[openj9-shrc]: https://eclipse.dev/openj9/docs/shrc/
[openj9-aot]: https://eclipse.dev/openj9/docs/aot/


## GraalVM Native Image

[GraalVM Native Image][native-image] compiles Java or Scala code ahead-of-time to a native executable. A native image
executable provides lower resource usage compared with the JVM, smaller deployments, faster starts, and immediate peak
performance — making it ideal for Akka Edge deployments in resource-constrained environments and for responsiveness
under autoscaling.

Native Image builds can be integrated into your application using build tool plugins:

* [Maven plugin for GraalVM Native Image](https://graalvm.github.io/native-build-tools/latest/maven-plugin.html)
* [Gradle plugin for GraalVM Native Image](https://graalvm.github.io/native-build-tools/latest/gradle-plugin.html)
* [sbt plugin for GraalVM Native Image](https://github.com/scalameta/sbt-native-image)

Native Image builds need to be configured in various ways. See the build tool plugins and the [Native Image build
configuration][native-image-configuration] reference documentation for more information on how to do this. An important
part of the configuration is the _reachability metadata_, which covers dynamic features used at runtime and which can't
be discovered statically at build time.

### GraalVM tracing agent

GraalVM provides a [tracing agent][native-image-tracing] to automatically gather metadata and create configuration
files. The tracing agent tracks usage of dynamic features during regular running of the application in a JVM, and
outputs Native Image configuration based on the code paths that were exercised. The build tool plugins for Native Image
provide ways to run locally with the tracing agent enabled. It can also be useful to deploy your application to a
testing environment with the GraalVM tracing agent enabled, to capture usage in an actual deployment environment and
exercising all the Akka Edge features that are being used.

@@@ note
The GraalVM Native Image tracing agent can only generate configuration for code paths that were observed during the
running of an application. When using this approach for generating configuration, make sure that tests exercise all
possible code paths. In particular, check dynamic serialization of classes used for persistence or cross-node
communication.
@@@

### GraalVM Native Image build examples

The Local Drone Control service from the @ref[Akka Edge guide](guide.md) has been configured for GraalVM Native Image
as an example:

* [Native Image build for Local Drone Control service in Java](https://github.com/akka/akka-projection/blob/main/samples/grpc/local-drone-control-java/native-image/README.md)
* [Native Image build for Local Drone Control service in Scala](https://github.com/akka/akka-projection/blob/main/samples/grpc/local-drone-control-scala/native-image/README.md)

[native-image]: https://www.graalvm.org/latest/reference-manual/native-image/
[native-image-configuration]: https://www.graalvm.org/latest/reference-manual/native-image/overview/BuildConfiguration/
[native-image-tracing]: https://www.graalvm.org/latest/reference-manual/native-image/metadata/AutomaticMetadataCollection/


## Multidimensional autoscaling

An application using Akka Edge features, such as event sourcing and projections over gRPC, cannot scale to zero when
idle. It's possible, however, for the application to be scaled to and from "near zero" — scaling down to a state of
minimal resource usage when idle, scaling up and out when load is increased. Multidimensional autoscaling is scaling
both vertically (lower or higher resource allocation) and horizontally (fewer or more instances) and can be used to
align resource usage with the actual demand given dynamic workloads.

In Kubernetes, the [horizontal pod autoscaler (HPA)][hpa] and the [vertical pod autoscaler (VPA)][vpa] can be combined,
so that when the service is idle it is both _scaled down_ with minimal resource requests, and _scaled in_ to a minimal
number of pods.

A multidimensional autoscaling configuration for an Akka Edge application in Kubernetes can be set up with:

* Custom VPA recommender for vertical autoscaling configured to respond quickly, to "activate" the application. The
  default vertical pod autoscaler bases its recommendations for resource requests over long time frames (over days). A
  custom VPA recommender is needed to go from minimal resource allocation to higher requests more quickly.

* HPA configured to horizontally autoscale based on [custom metrics] — such as the number of active event sourced
  entities in an Akka Cluster. Custom metrics need to be exposed by the application and configured for the Kubernetes
  custom metrics API with an "adapter", such as the [Prometheus adapter].

* Application availability ensured by having a minimum of 2 replicas, and configuring a [pod disruption budget
  (PDB)][pdb] so that no more than one pod is unavailable at a time. When the vertical autoscaler makes changes, pods
  are evicted and restarted with updated resource requests. In-place changes are not currently supported by Kubernetes.

@@@ note
The Kubernetes horizontal and vertical pod autoscalers should not be triggered using the same metrics. As the default
vertical autoscaler is currently designed for resource metrics (CPU and memory), the horizontal autoscaler should be
configured to use custom metrics.
@@@

### Multidimensional autoscaling examples

The Local Drone Control service from the @ref[Akka Edge guide](guide.md) has been configured for multidimensional
autoscaling. The example uses GraalVM Native Image builds for low resource usage, combines the vertical and horizontal
pod autoscalers, and runs in k3s (lightweight Kubernetes).

* [Multidimensional autoscaling example for Local Drone Control service in Java](https://github.com/akka/akka-projection/blob/main/samples/grpc/local-drone-control-java/autoscaling/README.md)
* [Multidimensional autoscaling example for Local Drone Control service in Scala](https://github.com/akka/akka-projection/blob/main/samples/grpc/local-drone-control-scala/autoscaling/README.md)

[hpa]: https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/
[vpa]: https://github.com/kubernetes/autoscaler/tree/master/vertical-pod-autoscaler
[custom metrics]: https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/#scaling-on-custom-metrics
[Prometheus adapter]: https://github.com/kubernetes-sigs/prometheus-adapter
[pdb]: https://kubernetes.io/docs/tasks/run-application/configure-pdb/
