# Running the Projection

@@@ note

This example requires a Cassandra database to run. 
If you do not have a Cassandra database then you can run one locally as a Docker container.
To run a Cassandra database locally you can use [`docker-compose`](https://docs.docker.com/compose/) to run the [`docker-compose.yaml`](https://github.com/akka/akka-projection/blob/master/docker-compose.yml) found in the Projections project root.
The `docker-compose.yml` file references the latest [Cassandra Docker Image](https://hub.docker.com/_/cassandra).

```shell
$ docker-compose --project-name cassandra-projections up -d cassandra
Creating network "cassandra-projections_default" with the default driver
Creating cassandra-projections_cassandra_1 ... done
```

To get a `cqlsh` prompt run another Cassandra container in interactive mode using the same network (`cassandra-projections_default`) as the container currently running.

```shell
$ docker run -it --network cassandra-projections_default --rm cassandra cqlsh cassandra 
Connected to Test Cluster at cassandra:9042.
[cqlsh 5.0.1 | Cassandra 3.11.6 | CQL spec 3.4.4 | Native protocol v4]
Use HELP for help.
cqlsh> 
```

To use a different Cassandra database update the [Cassandra driver's contact-points configuration](https://doc.akka.io/docs/akka-persistence-cassandra/current/configuration.html#contact-points-configuration) found in `./examples/src/resources/guide-shopping-cart-app.conf`.

@@@

To run the Projection we must setup our Cassandra database to support the Cassandra Projection offset store as well as the new tables we are projecting into with the `DailyCheckoutProjectionHandler`.

Create a Cassandra keyspace.

```
CREATE KEYSPACE IF NOT EXISTS akka_projection WITH REPLICATION = { 'class' : 'SimpleStrategy','replication_factor':1 };
```

Create the Cassandra Projection offset store table.
The DDL can be found in the @ref:[Cassandra Projection, Schema section](../cassandra.md#schema).

Create the `DailyCheckoutProjectionHandler` projection tables with the DDL found below.

```
CREATE TABLE IF NOT EXISTS akka_projection.cart_state (
cart_id text,
item_id text,
quantity int,
PRIMARY KEY (cart_id, item_id));

CREATE TABLE IF NOT EXISTS akka_projection.daily_checkouts (
date date,
cart_id text,
item_id text,
quantity int,
PRIMARY KEY (date, cart_id, item_id));
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

After a few seconds you should see the `DailyCheckoutProjectionHandler` logging that displays the current checkouts for the day:

```shell
[2020-07-16 15:26:23,420] [INFO] [docs.guide.DailyCheckoutProjectionHandler] [] [ShoppingCartApp-akka.actor.default-dispatcher-5] - DailyCheckoutProjectionHandler(carts-eu) current checkouts for the day [2020-07-16] is:                                                                                                 
Date        Cart ID  Item ID             Quantity                                                                                                             
2020-07-16  018db    akka t-shirt        0                                                                                                                    
2020-07-16  018db    cat t-shirt         2                                                                                                                    
2020-07-16  01ef3    akka t-shirt        1                                                                                                                    
2020-07-16  05747    bowling shoes       1                                                                                                                    
2020-07-16  064a0    cat t-shirt         1                                                                                                                    
2020-07-16  064a0    skis                0             
...
```

Use the CQL shell to observe the same information in the `daily_checkouts` table.

```
cqlsh:akka_projection> select cart_id, item_id, quantity from akka_projection.daily_checkouts where date = '2020-07-16' limit 10;

cart_id | item_id       | quantity
---------+---------------+----------
018db |  akka t-shirt |        0
018db |   cat t-shirt |        2
01ef3 |  akka t-shirt |        1
03e7e | bowling shoes |        1
03e7e |   cat t-shirt |        1
05747 | bowling shoes |        1
064a0 |   cat t-shirt |        1
064a0 |          skis |        0
06602 |          skis |        0
084e1 |  akka t-shirt |        2

(10 rows)
```
