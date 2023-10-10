# Autoscaling example

This example demonstrates multidimensional autoscaling, to scale the Local Drone Control service to
and from "near zero" — scaling down to a state of minimal resource usage when idle, scaling up and
out when load is increased.

The example uses GraalVM Native Image builds for low resource usage, combines the Kubernetes
vertical and horizontal pod autoscalers, and runs in a k3s cluster (lightweight Kubernetes).


## Requirements

The following tools are required to run this example locally:

- [docker](https://www.docker.com) - Docker engine for building and running containers
- [kubectl](https://kubernetes.io/docs/reference/kubectl) - Kubernetes command line tool
- [k3d](https://k3d.io) - k3s (lightweight Kubernetes) in Docker
- [helm](https://helm.sh) - package manager for Kubernetes


## Build local-drone-control Docker image

First build a Docker image for the Local Drone Control service, as a native image and configured to
run as a multi-node Akka Cluster with PostgreSQL. From the `local-drone-control-java` directory:

```
docker build -f native-image/Dockerfile --build-arg profile=clustered -t local-drone-control .
```

See the native-image build for more information.


## Run the Central Drone Control service

Run the Central Drone Control service. By default, the example assumes this is running locally, but
it can also be deployed.

To run locally, from the `restaurant-drone-deliveries-service-java` directory:

```
docker compose up --wait

docker exec -i postgres_db psql -U postgres -t < ddl-scripts/create_tables.sql

mvn compile exec:exec -DAPP_CONFIG=local1.conf
```

Or see the documentation for deploying to Kubernetes in a cloud environment.


## Start the Local Drone Control service in k3s

A convenience script starts a k3d cluster (k3s cluster in Docker), installs the infrastructure
dependencies for persistence, monitoring, and autoscaling, and then installs the Local Drone
Control service configured for multidimensional autoscaling.

To start the Local Drone Control service in a local k3s cluster, run the `up.sh` script:

```
autoscaling/local/up.sh
```

If the Central Drone Control service has been deployed somewhere other than locally on
`localhost:8101`, the connection details can be specified using arguments to the script:

```
autoscaling/local/up.sh --central-host deployed.app --central-port 443 --central-tls true
```


## Autoscaling infrastructure

This example uses multidimensional autoscaling, combining the Kubernetes vertical and horizontal
pod autoscalers, so that when the service is idle it is both _scaled down_ with minimal resource
requests, and _scaled in_ to a minimal number of pods. The same metrics should not be used for both
the vertical and horizontal autoscalers, so the horizontal pod autoscaler is configured to use a
custom metric — the number of active drones. When activity for the service increases, the vertical
pod autoscaler (VPA) will increase the resource requests, and when the number of active drones
increases, the horizontal pod autoscaler (HPA) will increase the number of pods in the deployment.

The default vertical pod autoscaler recommends new resource requests and limits over long time
frames. In this example, a custom VPA recommender has been configured for short cycles and metric
history, to scale up quickly. The horizontal scaling has been configured for minimum 2 replicas, to
ensure availability of the service (when pods are recreated on vertical scaling), and a pod
disruption budget has been configured to ensure that no more than one pod is unavailable at a time.

You can see the current state and recommendations for the autoscalers by running:

```
kubectl get hpa,vpa
```


## Simulate drone activity

A simple load simulator is available, to demonstrate autoscaling behavior given increasing load.

This simulator moves drones on random delivery paths, frequently reporting updated locations.

In the `autoscaling/simulator` directory, run the Gatling load test:

```
mvn gatling:test
```

You can see the current resource usage for pods by running:

```
kubectl top pods
```

And the current state of the autoscalers and deployed pods with:

```
kubectl get hpa,vpa,deployments,pods
```

The vertical pod autoscaler will increase the resource requests for pods as needed. The current CPU
requests for pods can be seen by running:

```
kubectl get pods -o custom-columns='NAME:metadata.name,CPU:spec.containers[].resources.requests.cpu'
```

When the simulated load has finished, and idle entities have been passivated, the autoscalers will
eventually scale the service back down.


## Stop the Local Drone Control service

To stop and delete the Local Drone Control service and k3s cluster, run the `down.sh` script:

```
autoscaling/local/down.sh
```
