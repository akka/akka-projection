## Running the sample code

1. Start two local PostgresSQL servers, on ports 5101 and 5201. The included `docker-compose.yml` starts everything required for running locally.

    ```shell
    docker-compose up --build --detach --wait

    # creates the tables needed for Akka Persistence
    # as well as the offset store table for Akka Projection
    docker exec -i postgres_db_1 psql -U postgres -t < ddl-scripts/create_tables.sql
    docker exec -i postgres_db_2 psql -U postgres -t < ddl-scripts/create_tables.sql
    ```

2. Make sure you have compiled the project

    ```shell
    mvn compile 
    ```

3. Start a first node for each replica:

    ```shell
    mvn compile exec:exec -DAPP_CONFIG=replica1-local1.conf
    ```

    ```shell
    mvn compile exec:exec -DAPP_CONFIG=replica2-local1.conf
    ```

4. (Optional) Start another node with different ports:

    ```shell
    mvn compile exec:exec -DAPP_CONFIG=replica1-local2.conf
    ```

    ```shell
    mvn compile exec:exec -DAPP_CONFIG=replica2-local2.conf
    ```

5. (Optional) More can be started:

    ```shell
    mvn compile exec:exec -DAPP_CONFIG=replica1-local3.conf
    ```

    ```shell
    mvn compile exec:exec -DAPP_CONFIG=replica2-local3.conf
    ```

6. Check for service readiness

    ```shell
    curl http://localhost:9101/ready
    ```

    ```shell
    curl http://localhost:9201/ready
    ```

7. Try it with [grpcurl](https://github.com/fullstorydev/grpcurl):

    ```shell
    # add item to cart on the first replica
    grpcurl -d '{"cartId":"cart1", "itemId":"socks", "quantity":7}' -plaintext 127.0.0.1:8101 shoppingcart.ShoppingCartService.AddItem

    # get cart from first replica
    grpcurl -d '{"cartId":"cart1"}' -plaintext 127.0.0.1:8101 shoppingcart.ShoppingCartService.GetCart

    # get cart from second replica
    grpcurl -d '{"cartId":"cart1"}' -plaintext 127.0.0.1:8201 shoppingcart.ShoppingCartService.GetCart

    # update quantity of item on the second replica
    grpcurl -d '{"cartId":"cart1", "itemId":"socks", "quantity":2}' -plaintext 127.0.0.1:8201 shoppingcart.ShoppingCartService.RemoveItem

    # check out cart
    grpcurl -d '{"cartId":"cart1"}' -plaintext 127.0.0.1:8101 shoppingcart.ShoppingCartService.Checkout
    ```

    or same `grpcurl` commands to port 8102/8202 to reach node 2.
    ```

8. To enable filters you can `Main` to use `ShoppingCart.initWithProducerFilter` instead of `ShoppingCart.init`. Then you have to mark the cart as "wip" for replication to take place:

    ```shell
    grpcurl -d '{"cartId":"cart1"}' -plaintext 127.0.0.1:8101 shoppingcart.ShoppingCartService.MarkCustomerVip
   ```
