## Running the sample code

1. Start a local PostgresSQL server on default port 5432. The included `docker-compose.yml` starts everything required for running locally. Note that for convenience this service and the shopping cart service is sharing the same database, in an actual service consuming events the consuming services are expected to have their own separate databases.

    ```shell
    docker-compose up --wait

    # creates the tables needed for Akka Persistence
    # as well as the offset store table for Akka Projection
    docker exec -i postgres_db psql -U postgres -t < ddl-scripts/create_tables.sql
    ```

2. Start a first node:

    ```shell 
    sbt -Dconfig.resource=local1.conf run
    ```

3. Start `shopping-cart-service` and add item to cart

4. Add at least a total quantity of 10 to the cart, smaller carts are excluded by the event filter.

5. Notice the log output in the terminal of the `shopping-analytics-service`
