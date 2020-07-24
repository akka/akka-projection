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
[2020-07-23 16:12:10,667] [INFO] [docs.guide.CheckoutProjectionHandler] [] [ShoppingCartApp-akka.actor.default-dispatcher-19] - CheckoutProjectionHandler(shopping-cart) last [10] checkouts: 
Cart ID     Event Time
75434       2020-07-23T20:11:44.606533Z
c9b3b       2020-07-23T20:11:45.626023Z
7df24       2020-07-23T20:11:46.646422Z
f177a       2020-07-23T20:11:47.666142Z
78646       2020-07-23T20:11:48.686501Z
2bc98       2020-07-23T20:11:49.706239Z
5fe8b       2020-07-23T20:11:50.726491Z
603f9       2020-07-23T20:11:51.746403Z
8e0d8       2020-07-23T20:11:52.766602Z
be615       2020-07-23T20:11:53.785896Z
```

Use the CQL shell to observe the same information in the `cart_checkout_state` table.

```
cqlsh> select cart_id, last_updated, checkout_time from akka_projection.cart_checkout_state limit 10;

 cart_id | last_updated                    | checkout_time
---------+---------------------------------+---------------------------------
   729cb | 2020-07-23 20:12:10.640000+0000 | 2020-07-23 20:11:42.566000+0000
   e64e1 | 2020-07-23 20:12:10.703000+0000 | 2020-07-23 20:12:09.086000+0000
   e86b5 | 2020-07-23 20:12:10.642000+0000 | 2020-07-23 20:11:43.586000+0000
   78646 | 2020-07-23 20:12:10.655000+0000 | 2020-07-23 20:11:48.686000+0000
   c58a0 | 2020-07-23 20:12:10.628000+0000 | 2020-07-23 20:11:38.486000+0000
   347ac | 2020-07-23 20:12:10.671000+0000 | 2020-07-23 20:11:56.846000+0000
   90b08 | 2020-07-23 20:12:10.705000+0000 | 2020-07-23 20:12:10.106000+0000
   75434 | 2020-07-23 20:12:10.645000+0000 | 2020-07-23 20:11:44.606000+0000
   8e0d8 | 2020-07-23 20:12:10.662000+0000 | 2020-07-23 20:11:52.766000+0000
   603f9 | 2020-07-23 20:12:10.660000+0000 | 2020-07-23 20:11:51.746000+0000

(10 rows)
```
