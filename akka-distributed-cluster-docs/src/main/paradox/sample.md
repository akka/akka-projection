# Sample - Distributed Shopping Cart

For a more hands on sample we have implemented to variations on a distributed shopping cart, where a user can add and
remove items and finally check the selected goods out. This is a somewhat synthetic example, but, it is a domain we expect 
you will quickly understand, so that we can focus on explaining the various features of Akka Distributed Cluster. 

## Active-active

For an Active-Active shopping cart, the same service will run in different data centers or cloud regions - each called a replica. 
The carts will be replicated and exist in all replicas, unless filtering is applied.

### Filtering

The replicated shopping cart sample showcases filtering on the producer side. The cart events are tagged with "small", 
"medium" or "large" depending on the number of items in the cart. 

FIXME tagger snippet

Replicating events is only done if a cart is tagged as "medium" or "large":

FIXME producer filter snippet

The sample also contains a consumer side filter, to explicitly exclude specific shopping cart ids requested by a client.

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

The Service to Service eventing sample showcases filtering in two ways, one controlled on the producing side, one on the 
consuming side. The combined filtering doesn't make much sense, see this as an example of two different ways to do achieve
the same thing, but placing the control over it in the producing service or the consuming service.

The shopping cart service is set up with a producer filter to only pass carts that has been tagged with the tag `medium` or `large`,
triggered by the number of items in the cart:

FIXME snippet

The analytics service is set up to not consume all shopping carts from the upstream shopping cart service but only include
carts containing 10 or more items.

FIXME snippet

### Deploying with Kubernetes and connecting services

TODO

* Kubernetes ingress config
* mTLS for secure communication between replicas
* Databases?
