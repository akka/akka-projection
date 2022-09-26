## Running the sample code

1. Start a local PostgresSQL server on default port 5432. The included `docker-compose.yml` starts everything required for running locally.

    ```shell
    docker-compose up -d

    # creates the tables needed for Akka Persistence
    # as well as the offset store table for Akka Projection
    docker exec -i shopping-cart-service-java-postgres-db-1 psql -U postgres -t < ddl-scripts/create_tables.sql
    ```

2. Start a first node:

    ```
    mvn compile exec:exec -DAPP_CONFIG=local1.conf
    ```

3. Start `shopping-cart-service` and add item to cart

4. Notice the log output in the terminal of the `shopping-analytics-service`