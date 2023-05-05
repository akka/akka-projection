# Part 4: Deploying with Kubernetes

FIXME bla bla a namespace `shopping-cart-namespace`

JSON
:  @@snip [ShoppingCart.java](/samples/replicated/shopping-cart-service-java/kubernetes/namespace.json) { }

FIXME bla bla pre-requisite for Akka management

YAML
:  @@snip [ShoppingCart.java](/samples/replicated/shopping-cart-service-java/kubernetes/akka-cluster-roles.yml) { }

FIXME bla bla the service itself

YAML
:  @@snip [ShoppingCart.java](/samples/replicated/shopping-cart-service-java/kubernetes/akka-cluster.yml) { }

FIXME bla bla database password in secrets, a PostgreSQL instance of some kind, schema not managed by service itself

FIXME something about mTLS for the replication/s2s event streams