## Part 2: Service to Service eventing

To implement Service to Service eventing, we will use two services, the shopping cart defined in the previous step and 
a downstream analytics service. 

Each of the services have their own lifecycle and are deployed separately, possibly in different data centers or
cloud regions.

### Publish the events of the shopping cart

FIXME

### Consume events

FIXME

### Filters

The Service to Service eventing sample showcases filtering in two ways, one controlled on the producing side, one on the
consuming side. The combined filtering doesn't make much sense, see this as an example of two different ways to do achieve
the same thing, but placing the control over it in the producing service or the consuming service.

The shopping cart service is set up with a producer filter to only pass carts that has been tagged with the tag `medium` or `large`,
triggered by the number of items in the cart:

FIXME snippet

The analytics service is set up to not consume all shopping carts from the upstream shopping cart service but only include
carts containing 10 or more items.

FIXME snippet

## What's next?

* Turning the shopping cart highly available through Active-Active