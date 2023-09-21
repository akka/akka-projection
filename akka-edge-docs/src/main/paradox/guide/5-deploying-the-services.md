# Deploying the Restaurant Delivery Service

In this part we will deploy @ref:[Restaurant Deliveries Service](3-restaurant-deliveries-service.md) to Kubernetes.

## Setup Kubernetes cluster

Use instructions from your preferred cloud provider of how to start two Kubernetes clusters in two separate regions.

As an example, we use Amazon EKS in regions `us-east-2`:

```
eksctl create cluster \
  --name eks-akka-edge-us-east-2 \
  --version 1.24 \
  --region us-east-2 \
  --nodegroup-name linux-nodes \
  --node-type m5.xlarge \
  --nodes 3 \
  --nodes-min 1 \
  --nodes-max 4 \
  --with-oidc \
  --managed
```

## Setup database

Use instructions from your preferred cloud provider of how to run a PostgreSQL database in each of the two regions.

As an example, we use [Amazon RDS Postgres](https://console.aws.amazon.com/rds/home). For a trial PostgreSQL you can
select the following aside from defaults:

- Standard create
- PostgreSQL
- Free tier (some regions don't offer free tier)
- DB instance identifier: `restaurant-drone-deliveries-db`
- Master password: `<a password>`
- Turn off storage autoscaling
- VPC: Use the same as your EKS cluster is running in
- Create new VPC security group: `restaurant-drone-deliveries-sg`
- Turn off Automatic Backups in the Additional Configuration section.

To allow the nodes in the EKS cluster to connect to the database you have to add a rule in the security group.

Go to the [VPC console](https://console.aws.amazon.com/vpc/home). Select "Security Groups".

There are 3 security groups for the EKS cluster, you should select the one with description "EKS created security group ...". The one that has a name that doesn't contain `ControlPlaneSecurityGroup` and doesn't contain `ClusterSharedNodeSecurityGroup` . Make a note of this security group id for the EKS cluster.

Go back to the [Amazon RDS console](https://console.aws.amazon.com/rds/home#databases). Select the database that you created. Click on the "VPC security groups" in the tab "Connectivity & security".

Edit inbound rules > add rule > Custom TCP > Port 5432 > Source custom. Add the security group for the EKS cluster. Save rules.

More details in [A DB instance in a VPC accessed by an EC2 instance in the same VPC](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_VPC.Scenarios.html#USER_VPC.Scenario1)

## Namespace

Define a Kubernetes namespace with a `namespace.json` file:

JSON
:  @@snip [namespace.json](/samples/grpc/restaurant-drone-deliveries-service-scala/kubernetes/namespace.json) { }

Create the namespace with:

```
kubectl -f apply kubernetes/namespace.json
```

For convenience, you can use this namespace by default:

```
kubectl config set-context --current --namespace=restaurant-drone-deliveries-namespace
```

## Role based access control

For Akka Cluster bootstrap we need to define access control with a `rbac.yml` file:

YAML
:  @@snip [rbac.yml](/samples/grpc/restaurant-drone-deliveries-service-scala/kubernetes/rbac.yml) { }

Apply the access control with:

```
kubectl apply -f kubernetes/rbac.yml
```

## Database secret

Create a Kubernetes secret with the connection details and credentials to the database:

```
kubectl create secret generic \
    db-secret \
    --from-literal=DB_HOST=restaurant-drone-deliveries-db.cgrtpi2lqrw8.us-east-2.rds.amazonaws.com \
    --from-literal=DB_USER=postgres \
    --from-literal=DB_PASSWORD=<the password>
```

## Database schema

You can run `psql` from the Kubernetes cluster with

```
kubectl run -i --tty db-mgmt --image=postgres --restart=Never --rm \
  --env=PGPASSWORD=<the password> -- \
  psql -h restaurant-drone-deliveries-db.cgrtpi2lqrw8.us-east-2.rds.amazonaws.com -U postgres
```

Paste the DDL statements from the `ddl-scripts/create-tables.sql` to the `psql` prompt:

SQL
:  @@snip [create-tables.sql](/samples/grpc/restaurant-drone-deliveries-service-scala/ddl-scripts/create_tables.sql) { }

## Deploy the Restaurant Drone Deliveries Service

We are now going to deploy the `restaurant-drone-deliveries-service` to the created kubernetes cluster in `us-east-2`.

This step is for deploying:

* Java: https://github.com/akka/akka-projection/tree/main/samples/grpc/shopping-cart-service-java
* Scala: https://github.com/akka/akka-projection/tree/main/samples/grpc/shopping-cart-service-scala

Build and publish the docker image to docker.io:

Scala
:  ```
sbt -Ddocker.username=<username> -Ddocker.registry=docker.io docker:publish
```

Java
:  ```
mvn -DskipTests -Ddocker.registry=<username>/restaurant-drone-deliveries-service clean package docker:push
```

The Kubernetes `Deployment` in `deployment.yml` file:

YAML
:  @@snip [deployment.yml](/samples/grpc/restaurant-drone-deliveries-service-scala/kubernetes/deployment.yml) { }

Update the `image:` in the `deployment.yml` with the specific image version and location you published.

```
kubectl apply -f kubernetes/deployment.yml
```

### Port forward

Create a Kubernetes `Service` and port forward to simplify access to the pods from your local machine:

YAML
:  @@snip [deployment.yml](/samples/grpc/shopping-cart-service-scala/kubernetes/service.yml) { }

```
kubectl apply -f kubernetes/service.yml
```

Start port forward with:

```
kubectl port-forward svc/restaurant-drone-deliveries-service-svc 8101:8101
```

### Exercise the service

Use [grpcurl](https://github.com/fullstorydev/grpcurl) to exercise the service.

Set up two restaurants: 

```shell
grpcurl -d '{"restaurant_id":"restaurant1","coordinates":{"latitude": 59.330324, "longitude": 18.039568}, "local_control_location_id": "sweden/stockholm/kungsholmen" }' -plaintext localhost:8101 central.deliveries.RestaurantDeliveriesService.SetUpRestaurant
grpcurl -d '{"restaurant_id":"restaurant2","coordinates":{"latitude": 59.342046, "longitude": 18.059095}, "local_control_location_id": "sweden/stockholm/norrmalm" }' -plaintext localhost:8101 central.deliveries.RestaurantDeliveriesService.SetUpRestaurant
```

Register a delivery for each restaurant:

```shell
grpcurl -d '{"restaurant_id":"restaurant1","delivery_id": "order1","coordinates":{"latitude": 59.330841, "longitude": 18.038885}}' -plaintext localhost:8101 central.deliveries.RestaurantDeliveriesService.RegisterDelivery
grpcurl -d '{"restaurant_id":"restaurant2","delivery_id": "order2","coordinates":{"latitude": 59.340128, "longitude": 18.056303}}' -plaintext localhost:8101 central.deliveries.RestaurantDeliveriesService.RegisterDelivery
```

We have not set up any local drone delivery services yet so there are no drones to actually pick up the orders.


## Load balancer

To access the `restaurant-drone-deliveries-service` from `local-drone-control` services running in other regions or on the edge
we need an Internet facing load balancer.

There are many alternatives for secure access with a load balancer. An incomplete list of options:

* Network load balancer such as [AWS Load Balancer Controller](https://docs.aws.amazon.com/eks/latest/userguide/network-load-balancing.html) with TLS all the way between the services.
* Application load balancer such as [AWS Load Balancer Controller](https://docs.aws.amazon.com/prescriptive-guidance/latest/patterns/deploy-a-grpc-based-application-on-an-amazon-eks-cluster-and-access-it-with-an-application-load-balancer.html), which terminates TLS.
* [NGINX Ingress Controller](https://docs.nginx.com/nginx-ingress-controller/)
* [Contour](https://projectcontour.io)
* [Linkerd multi-cluster](https://linkerd.io/2.13/features/multicluster/)

Mutual authentication with TLS (mTLS) can be very useful where only other known services are allowed to interact with
a service, and public access should be denied. See [Akka gRPC documentation](https://doc.akka.io/docs/akka-grpc/current/mtls.html).

We are going to use a network load balancer for simplicity of this example, and we are not using TLS. Real applications would of course require TLS.

Follow the instructions of how to [install the AWS Load Balancer Controller add-on](https://docs.aws.amazon.com/eks/latest/userguide/aws-load-balancer-controller.html).

Create a Kubernetes `Service` that is configured for the load balancer:

YAML
:  @@snip [service-nlb.yml](/samples/grpc/restaurant-drone-deliveries-service-scala/kubernetes/service-nlb.yml) { }

The external DNS name is shown by:

```
kubectl get services
```



## Deploy local drone control instances

This step is for deploying:

* Java: https://github.com/akka/akka-projection/tree/main/samples/grpc/local-drone-control-java
* Scala: https://github.com/akka/akka-projection/tree/main/samples/grpc/local-drone-control-scala

Create a Kubernetes `Secret` with the gRPC connection details for the load balancers:

```
kubectl create secret generic \
    local-drone-control-secret \
    --from-literal=CENTRAL_DRONE_CONTROL_HOST=k8s-restaurant-drone-deliveries-19708e1324-24617530ddc6d2cb.elb.us-east-2.amazonaws.com \
    --from-literal=CENTRAL_DRONE_CONTROL_PORT=80 \
    --from-literal=LOCATION_ID=sweden/stockholm/kungsholmen
```


FIXME the rest here onwards is half done

This secret is referenced from configuration environment variables from the `deployment.yml`:

YAML
:  @@snip [deployment.yml](/samples/replicated/local-drone-control-scala/kubernetes/deployment.yml) { }


Build the image and deploy in the same way as described previously.

Scala
:  ```
sbt -Ddocker.username=<username> -Ddocker.registry=docker.io docker:publish
```

Java
:  ```
mvn -DskipTests -Ddocker.registry=<username>/shopping-cart-service clean package docker:push
```

Update the `image:` in the `deployment.yml` with the specific image version and location you published.

```
kubectl -f apply kubernetes/debployment.yml
```

### Port forwards

Create a Kubernetes `Service` and port forward to simplify access to the pods from your local machine:

YAML
:  @@snip [deployment.yml](/samples/replicated/shopping-cart-service-scala/kubernetes/service.yml) { }

```
kubectl -f apply kubernetes/service.yml
```

Start port forward with:

```
kubectl port-forward svc/shopping-cart-service-svc 8101:8101
```

Switch `kubectl` context to the other region and apply the `service.yml` and start the port forward on another port:

```
kubectl port-forward svc/shopping-cart-service-svc 8201:8101
```

### Exercise the local drone control

Use [grpcurl](https://github.com/fullstorydev/grpcurl) to exercise the service.

```
