## Running the sample code

1. The tests expect a locally running database.

It can be started with the docker-compose file in the docker folder:

```
docker-compose -f docker/docker-compose-postgres.yml up -d
```

```
docker exec -i docker-postgres-db-1 psql -U postgres -t < ddl-scripts/create_tables_postgres.sql
```

2. Change from CapturingAppender to STDOUT in logback-test.xml

3. Start a first shopping cart service:

    ```shell
    sbt -Dconfig.resource=cart-local1.conf "Test/runMain shopping.cart.Main"
    ```

4. Try it with [grpcurl](https://github.com/fullstorydev/grpcurl):

    ```shell
    # add item to cart
    grpcurl -d '{"cartId":"cart1", "itemId":"socks", "quantity":3}' -plaintext 127.0.0.1:8101 shopping.cart.ShoppingCartService.AddItem
    ```
5. Start a first shopping order service, which will consume the events from the shopping cart service:

    ```shell
    sbt -Dconfig.resource=order-local1.conf "Test/runMain shopping.order.Main"
    ```
   
6. You should be able to see "Consumed event" logging in the shopping order service when events are stored in the shopping cart service.
