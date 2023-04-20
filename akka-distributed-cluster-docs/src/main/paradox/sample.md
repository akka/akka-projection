# Sample - Distributed Shopping Cart

For a more hands on sample we have implemented to variations on a distributed shopping cart, where a user can add and
remove items and finally check the selected goods out. This is a somewhat synthetic example, but, it is a domain we expect 
you will quickly understand, so that we can focus on explaining the various features of Akka Distributed Cluster. 

## Active-active

For an Active-Active shopping cart, the same service will run in different data centers or cloud regions.

### Filtering

The replicated shoppig cart is set up to only replicate carts containing 10 or more items.

It also contains a consumer side filter, to explicitly exclude specific shopping cart ids requested by a client.

TODO do we just pull in the example code and duplicate the sample description from projection docs?

### Deploying with Kubernetes and connecting replicas

TODO

 * Kubernetes ingress config 
 * mTLS for secure communication between replicas
 * Database?



## Service to Service eventing

To show off Service to Service eventing, the sample contains two services, the shopping cart and a downstream analytics
service. Each of the services have their own lifecycle and are deployed separately, possibly in different data centers or
cloud regions.

### Filtering

The analytics service is set up to not consume all shopping carts from the upstream shopping cart service but only include
carts containing 10 or more items.

TODO code snippets

### Deploying with Kubernetes and connecting services

TODO

* Kubernetes ingress config
* mTLS for secure communication between replicas
* Databases?
