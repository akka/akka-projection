# Running the Projection

@@@ note

This example requires a Cassandra database to run. 
If you do not have a Cassandra database then you can run one locally as a Docker container.
To run a Cassandra database locally you can use [`docker-compose`](https://docs.docker.com/compose/) to run the [`docker-compose.yaml`](https://github.com/akka/akka-projection/blob/master/docker-compose.yml) found in the Projections project root.
The `docker-compose.yml` file references the latest [Cassandra Docker Image](https://hub.docker.com/_/cassandra).

```shell
$ docker-compose --project-name getting-started up -d cassandra
Creating network "getting-started_default" with the default driver
Creating getting-started_cassandra_1 ... done
```

To get a `cqlsh` prompt run another Cassandra container in interactive mode using the same network (`cassandra-projections_default`) as the container currently running.

```shell
$ docker run -it --network getting-started_default --rm cassandra cqlsh cassandra
Connected to Test Cluster at cassandra:9042.
[cqlsh 5.0.1 | Cassandra 3.11.6 | CQL spec 3.4.4 | Native protocol v4]
Use HELP for help.
cqlsh> 
```

To stop Cassandra use `docker-compose --project-name getting-started stop`. To delete the container's state use `docker-compose --project-name getting-started rm -f`.

To use a different Cassandra database update the [Cassandra driver's contact-points configuration](https://doc.akka.io/docs/akka-persistence-cassandra/current/configuration.html#contact-points-configuration) found in `./examples/src/resources/guide-shopping-cart-app.conf`.

@@@

To run the Projection we must setup our Cassandra database to support the Cassandra Projection offset store as well as the new tables we are projecting into with the `CheckoutProjectionHandler`.

Create a Cassandra keyspace.

```
CREATE KEYSPACE IF NOT EXISTS akka_projection WITH REPLICATION = { 'class' : 'SimpleStrategy','replication_factor':1 };
```

Create the Cassandra Projection offset store table.
The DDL can be found in the @ref:[Cassandra Projection, Schema section](../cassandra.md#schema).

Create the `CheckoutProjectionHandler` projection table with the DDL found below.

```
CREATE TABLE IF NOT EXISTS akka_projection.cart_checkout_state (
cart_id text,
last_updated timestamp,
checkout_time timestamp,
PRIMARY KEY (cart_id));
```

Source events are generated with the `EventGeneratorApp`.
This app is configured to use [Akka Persistence Cassandra](https://doc.akka.io/docs/akka-persistence-cassandra/current/index.html) to persist random `ShoppingCartApp.Events` to a journal.
It will checkout 10 shopping carts with random items and quantities every 10 seconds.
After 10 seconds it will increment the checkout time by 1 hour and repeat.
This app will also automatically create all the Akka Persistence infrastructure tables in the `akka` keyspace.
We won't go into any further detail about how this app functions because it falls outside the scope of Akka Projections.
To learn more about the writing events with [Akka Persistence see the Akka documentation](https://doc.akka.io/docs/akka/current/typed/index-persistence.html).

To run the `EventGeneratorApp` use the following sbt command.

```shell
sbt "examples/test:runMain docs.guide.EventGeneratorApp"
```

If you don't see any connection exceptions you should eventually see log lines produced with the event being written to the journal.

Ex)

<!-- FIXME: update when event generator app updated to persist to cart id persistenceids -->
```shell
[2020-07-16 15:13:59,855] [INFO] [docs.guide.EventGeneratorApp$] [] [EventGenerator-akka.actor.default-dispatcher-3] - persisting event ItemAdded(62e4e,bowling shoes,0) MDC: {persistencePhase=persist-evt, akkaAddress=akka://EventGenerator, akkaSource=akka://EventGenerator/user/persister, sourceActorSystem=EventGenerator, persistenceId=all-shopping-carts}
```

Finally, we can run the projection itself by using sbt to run `ShoppingCartApp`

```shell
sbt "examples/test:runMain docs.guide.ShoppingCartApp"
```

After a few seconds you should see the `CheckoutProjectionHandler` logging that displays the current checkouts for the day:

```shell
[2020-07-23 12:15:36,723] [INFO] [docs.guide.CheckoutProjectionHandler] [] [ShoppingCartApp-akka.actor.default-dispatcher-4] - CheckoutProjectionHandler(shopping-cart) last [10] checkouts: 
Cart ID     Event Time
2e389       2020-07-23T16:15:19.817630Z
b9e20       2020-07-23T16:15:19.817630Z
4b445       2020-07-23T16:15:19.817630Z
98934       2020-07-23T16:15:19.817630Z
8a177       2020-07-23T16:15:19.817630Z
13abe       2020-07-23T16:15:19.817630Z
e5880       2020-07-23T16:15:19.817630Z
5a1c6       2020-07-23T16:15:19.817630Z
14930       2020-07-23T16:15:19.817630Z
db5d4       2020-07-23T17:15:19.817630Z
```

Use the CQL shell to observe the same information in the `cart_checkout_state` table.

```
cqlsh> select cart_id, last_updated, checkout_time from akka_projection.cart_checkout_state limit 10;

 cart_id | last_updated                    | checkout_time
---------+---------------------------------+---------------------------------
   64256 | 2020-07-23 16:16:30.023000+0000 | 2020-07-23 22:15:19.817000+0000
   540ed | 2020-07-23 16:10:35.317000+0000 | 2020-07-31 14:37:16.790000+0000
   6f5af | 2020-07-23 15:57:53.977000+0000 | 2020-07-27 20:37:16.790000+0000
   058f0 | 2020-07-23 15:52:41.684000+0000 |                            null
   fd52b | 2020-07-23 16:10:35.234000+0000 | 2020-07-31 05:37:16.790000+0000
   05ab1 | 2020-07-23 15:57:53.848000+0000 | 2020-07-27 14:37:16.790000+0000
   b2e40 | 2020-07-23 15:52:41.639000+0000 |                            null
   4ab5d | 2020-07-23 15:52:41.285000+0000 |                            null
   cbf17 | 2020-07-23 15:52:41.626000+0000 |                            null
   f6007 | 2020-07-23 16:10:35.036000+0000 | 2020-07-30 06:37:16.790000+0000

(10 rows)
```
