## Running the sample code

1. Start a local PostgresSQL server on default port 5432. The included `docker-compose.yml` starts everything required for running locally.

    ```shell
    docker compose up --wait

    # creates the tables needed for Akka Persistence
    # as well as the offset store table for Akka Projection
    docker exec -i postgres_db psql -U postgres -t < ddl-scripts/create_tables.sql
    ```

2. Start a first node:

    ```shell
    mvn compile exec:exec -DAPP_CONFIG=local1.conf
    ```

3. (Optional) Start another node with different ports:

    ```shell
    mvn compile exec:exec -DAPP_CONFIG=local2.conf
    ```

4. (Optional) More can be started:

    ```shell
    mvn compile exec:exec -DAPP_CONFIG=local3.conf
    ```

5. Check for service readiness

    ```shell
    curl http://localhost:9101/ready
    ```

6. Try it with [grpcurl](https://github.com/fullstorydev/grpcurl). Register a sensor:

    ```shell
    grpcurl -d '{"sensor_entity_id":"1", "secret":"foo"}' -plaintext 127.0.0.1:8101 iot.registration.RegistrationService.Register
    ```

    or same `grpcurl` commands to port 8102 to reach node 2.

7. Consume events from edge by running `iot-service-rs` and send temperature updates to it.

8. Read current temperature: 

   ```shell
    grpcurl -d '{"sensor_entity_id":"1"}' -plaintext 127.0.0.1:8101 iot.temperature.SensorTwinService.GetTemperature
    ```

