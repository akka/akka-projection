# Sample - Distributed Shopping Cart

For a more hands on sample we have implemented to variations on a distributed shopping cart, where a user can add and
remove items and finally check the selected goods out. This is a somewhat synthetic example but it is a domain we expect 
you will quickly understand, so that we can focus on explaining the various features of Akka Distributed Cluster. 

## Active-active

For an Active-Active shopping cart, the same service will run in different data centers or cloud regions.

### Filtering

TODO

### Deploying with Kubernetes and Connecting Replicas

 * Kubernetes ingress config 
 * mTLS for secure communication between replicas

TODO

## Service to Service eventing

To show off Service to Service eventing, the sample contains two services, the shopping cart and a downstream analytics
service. Each of the services have their own lifecycle and are deployed separately, possibly in different data centers or
cloud regions.

### Filtering

TODO