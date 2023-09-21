# Part 4: Deploying with Kubernetes

In this part we will deploy @ref:[Service to Service eventing](2-service-to-service.md) and @ref:[Active-active](3-active-active.md)
parts of the guide to Kubernetes in two separate regions.

## Setup Kubernetes cluster

Use instructions from your preferred cloud provider of how to start two Kubernetes clusters in two separate regions.

As an example, we use Amazon EKS in regions `us-east-2` and `eu-central-1`:

```
eksctl create cluster \
  --name eks-akka-us-east-2 \
  --version 1.24 \
  --region us-east-2 \
  --nodegroup-name linux-nodes \
  --node-type m5.xlarge \
  --nodes 3 \
  --nodes-min 1 \
  --nodes-max 4 \
  --with-oidc \
  --managed

eksctl create cluster \
  --name eks-akka-eu-central-1 \
  --version 1.24 \
  --region eu-central-1 \
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
- DB instance identifier: `shopping-db-us-east-2`
- Master password: `<a password>`
- Turn off storage autoscaling
- VPC: Use the same as your EKS cluster is running in
- Create new VPC security group: `shopping-db-sg`
- Turn off Automatic Backups in the Additional Configuration section.

To allow the nodes in the EKS cluster to connect to the database you have to add a rule in the security group.

Go to the [VPC console](https://console.aws.amazon.com/vpc/home). Select "Security Groups".

There are 3 security groups for the EKS cluster and you should select the one with description "EKS created security group ...". The one that has a name that doesn't contain `ControlPlaneSecurityGroup` and doesn't contain `ClusterSharedNodeSecurityGroup` . Make a note of this security group id for the EKS cluster.

Go back to the [Amazon RDS console](https://console.aws.amazon.com/rds/home#databases). Select the database that you created. Click on the "VPC security groups" in the tab "Connectivity & security".

Edit inbound rules > add rule > Custom TCP > Port 5432 > Source custom. Add the security group for the EKS cluster. Save rules.

More details in [A DB instance in a VPC accessed by an EC2 instance in the same VPC](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_VPC.Scenarios.html#USER_VPC.Scenario1)

## Namespace

Define a Kubernetes namespace with a `namespace.json` file: 

JSON
:  @@snip [namespace.json](/samples/grpc/shopping-cart-service-scala/kubernetes/namespace.json) { }

Create the namespace with:

```
kubectl apply -f kubernetes/namespace.json
```

For convenience, you can use this namespace by default:

```
kubectl config set-context --current --namespace=shopping-cart-namespace
```

## Role based access control

For Akka Cluster bootstrap we need to define access control with a `rbac.yml` file:

YAML
:  @@snip [rbac.yml](/samples/grpc/shopping-cart-service-scala/kubernetes/rbac.yml) { }

Apply the access control with:

```
kubectl apply -f kubernetes/rbac.yml
```

## Database secret

Create a Kubernetes secret with the connection details and credentials to the database:

```
kubectl create secret generic \
    db-secret \
    --from-literal=DB_HOST=shopping-db-us-east-2.cgrtpi2lqrw8.us-east-2.rds.amazonaws.com \
    --from-literal=DB_USER=postgres \
    --from-literal=DB_PASSWORD=<the password>
```

## Database schema

You can run `psql` from the Kubernetes cluster with 

```
kubectl run -i --tty db-mgmt --image=postgres --restart=Never --rm \
  --env=PGPASSWORD=<the password> -- \
  psql -h shopping-db-us-east-2.cgrtpi2lqrw8.us-east-2.rds.amazonaws.com -U postgres
```

Paste the DDL statements from the `ddl-scripts/create-tables.sql` to the `psql` prompt:

SQL
:  @@snip [create-tables.sql](/samples/grpc/shopping-cart-service-scala/ddl-scripts/create_tables.sql) { }

## Repeat for the other region

If you haven't already, repeat the steps for namespace, rbac, database secret, and database schema in the other region.

## Deploy shopping-cart-service

We are going to deploy the `shopping-cart-service` in region `us-east-2`.

This step is for deploying:

* Java: https://github.com/akka/akka-projection/tree/main/samples/grpc/shopping-cart-service-java
* Scala: https://github.com/akka/akka-projection/tree/main/samples/grpc/shopping-cart-service-scala

Build the image:

Scala
:  ```
sbt -Ddocker.username=<username> -Ddocker.registry=docker.io docker:publish
```

Java
:  ```
mvn -DskipTests -Ddocker.registry=<username>/shopping-cart-service clean package docker:push
```

The Kubernetes `Deployment` in `deployment.yml` file:

YAML
:  @@snip [deployment.yml](/samples/grpc/shopping-cart-service-scala/kubernetes/deployment.yml) { }

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
kubectl port-forward svc/shopping-cart-service-svc 8101:8101
```

### Exercise the shopping-cart-service

Use [grpcurl](https://github.com/fullstorydev/grpcurl) to exercise the service.

Add 7 socks to a cart:
```
grpcurl -d '{"cartId":"cart1", "itemId":"socks", "quantity":7}' -plaintext 127.0.0.1:8101 shoppingcart.ShoppingCartService.AddItem
```

Add 3 t-shirts to the same cart:
```
grpcurl -d '{"cartId":"cart1", "itemId":"t-shirt", "quantity":3}' -plaintext 127.0.0.1:8101 shoppingcart.ShoppingCartService.AddItem
```

## Load balancer

To access the `shopping-cart-service` from `shopping-analytics-service` running in another region we need an Internet facing load balancer.

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
:  @@snip [deployment.yml](/samples/grpc/shopping-cart-service-scala/kubernetes/service-nlb.yml) { }

The external DNS name is shown by:

```
kubectl get services
```

## Deploy shopping-analytics-service

We are going to deploy the `shopping-analytics-service` in region `eu-central-1`.

This step is for deploying:

* Java: https://github.com/akka/akka-projection/tree/main/samples/grpc/shopping-analytics-service-java
* Scala: https://github.com/akka/akka-projection/tree/main/samples/grpc/shopping-analytics-service-scala

Build the image:

Scala
:  ```
sbt -Ddocker.username=<username> -Ddocker.registry=docker.io docker:publish
```

Java
:  ```
mvn -DskipTests -Ddocker.registry=<username>/shopping-analytics-service clean package docker:push
```

The Kubernetes `Deployment` in `deployment.yml` file:

YAML
:  @@snip [deployment.yml](/samples/grpc/shopping-analytics-service-scala/kubernetes/deployment.yml) { }

Update the image in the `deployment.yml` with the specific image version and location you published.

Update the `SHOPPING_CART_SERVICE_GRPC_HOST` and `SHOPPING_CART_SERVICE_GRPC_PORT` in `deployment.yml` to correspond to
the load balancer that you created, for example:

```
            - name: SHOPPING_CART_SERVICE_GRPC_HOST
              value: "k8s-shopping-shopping-b4157add0d-54780992c148fa88.elb.us-east-2.amazonaws.com"
            - name: SHOPPING_CART_SERVICE_GRPC_PORT
              value: "80"
```

Run the service:

```
kubectl apply -f kubernetes/debployment.yml
```

### Exercise the shopping-analytics-service

You can watch the logs of the `shopping-analytics-service` with

```
kubectl get pods
kubectl logs -f <pod name>
```

Add some more items to the cart or create more carts as described for the `shopping-cart-service`. In the logs of the
`shopping-analytics-service` you should see entries such as:

```
Projection [cart-events-cart-768-1023] consumed ItemQuantityAdjusted for cart cart2, changed 3 socks. Total [8] events.
```

## Deploy replicated shopping-cart-service

This step is for deploying:

* Java: https://github.com/akka/akka-projection/tree/main/samples/replicated/shopping-cart-service-java
* Scala: https://github.com/akka/akka-projection/tree/main/samples/replicated/shopping-cart-service-scala

Now there will be connections in both directions, so install the @ref:[load balancer](#load-balancer) in the other region too.

Create a Kubernetes `Secret` with the gRPC connection details for the load balancers:

Example for `us-east-2`:

```
kubectl create secret generic \
    replication-secret \
    --from-literal=SELF_REPLICA_ID=replica1 \
    --from-literal=REPLICA2_GRPC_HOST=k8s-shopping-shopping-19708e1324-24617530ddc6d2cb.elb.eu-central-1.amazonaws.com \
    --from-literal=REPLICA2_GRPC_PORT=80
```

Example for `eu-central-1`:

```
kubectl create secret generic \
replication-secret \
--from-literal=SELF_REPLICA_ID=replica2 \
--from-literal=REPLICA1_GRPC_HOST=k8s-shopping-shopping-b4157add0d-54780992c148fa88.elb.us-east-2.amazonaws.com \
--from-literal=REPLICA1_GRPC_PORT=80
```

This secret is referenced from configuration environment variables from the `deployment.yml`:

YAML
:  @@snip [deployment.yml](/samples/replicated/shopping-cart-service-scala/kubernetes/deployment.yml) { }


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
kubectl apply -f kubernetes/debployment.yml
```

### Port forwards

Create a Kubernetes `Service` and port forward to simplify access to the pods from your local machine:

YAML
:  @@snip [deployment.yml](/samples/replicated/shopping-cart-service-scala/kubernetes/service.yml) { }

```
kubectl apply -f kubernetes/service.yml
```

Start port forward with:

```
kubectl port-forward svc/shopping-cart-service-svc 8101:8101
```

Switch `kubectl` context to the other region and apply the `service.yml` and start the port forward on another port:

```
kubectl port-forward svc/shopping-cart-service-svc 8201:8101
```

### Exercise the replicated shopping-cart-service

Use [grpcurl](https://github.com/fullstorydev/grpcurl) to exercise the service.

Add 7 socks to a cart:
```
grpcurl -d '{"cartId":"cart2", "itemId":"socks", "quantity":7}' -plaintext 127.0.0.1:8101 shoppingcart.ShoppingCartService.AddItem
```

Add 3 t-shirts to the same cart but in the other region (port 8201):
```
grpcurl -d '{"cartId":"cart1", "itemId":"t-shirt", "quantity":3}' -plaintext 127.0.0.1:8201 shoppingcart.ShoppingCartService.AddItem
```

Retrieve the cart:
```
grpcurl -d '{"cartId":"cart2"}' -plaintext 127.0.0.1:8101 shoppingcart.ShoppingCartService.GetCart
grpcurl -d '{"cartId":"cart2"}' -plaintext 127.0.0.1:8201 shoppingcart.ShoppingCartService.GetCart
```
