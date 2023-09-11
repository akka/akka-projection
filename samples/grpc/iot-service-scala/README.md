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
    sbt -Dconfig.resource=local1.conf run
    ```

3. (Optional) Start another node with different ports:

    ```shell
    sbt -Dconfig.resource=local2.conf run
    ```

4. (Optional) More can be started:

    ```shell
    sbt -Dconfig.resource=local3.conf run
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

7. Consume events from edge:

    ```shell
    sbt "Test/runMain iot.EdgeApp"
   ```

8. After registration of a sensor the EdgeApp will simulate temperature readings, which will be consumed by the iot-service. Read current temperature, which should be updated with a random value: 

9. ```shell
    grpcurl -d '{"sensor_entity_id":"1"}' -plaintext 127.0.0.1:8101 iot.temperature.SensorTwinService.GetTemperature
    ```

