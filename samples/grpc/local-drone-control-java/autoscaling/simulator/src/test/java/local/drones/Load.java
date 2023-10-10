package local.drones;

import static com.github.phisgr.gatling.kt.grpc.GrpcDsl.*;
import static io.gatling.javaapi.core.CoreDsl.*;

import com.github.phisgr.gatling.kt.grpc.StaticGrpcProtocol;
import com.github.phisgr.gatling.kt.grpc.action.GrpcCallActionBuilder;
import com.google.protobuf.Empty;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.grpc.ManagedChannelBuilder;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;
import local.drones.proto.DroneServiceGrpc;
import local.drones.proto.ReportLocationRequest;

public class Load extends Simulation {

  static final Duration RAMP_TIME = Duration.ofMinutes(5);
  static final int MAX_NUMBER_OF_DRONES = 500;
  static final int NUMBER_OF_DELIVERIES_PER_DRONE = 2;

  static final Coordinates LOCATION = new Coordinates(59.33258, 18.0649);
  static final int START_RADIUS = 1000; // metres
  static final int DESTINATION_RADIUS = 5000; // metres

  // 2m / 100ms = 72 km/hour
  static final Duration REPORT_EVERY = Duration.ofMillis(100);
  static final int TRAVEL_DISTANCE = 2; // metres

  final Config config = ConfigFactory.load().getConfig("local-drone-control");
  final String host = config.getString("host");
  final int port = config.getInt("port");
  final boolean tls = config.getBoolean("tls");

  final ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forAddress(host, port);
  final ManagedChannelBuilder<?> channelBuilder = tls ? builder : builder.usePlaintext();
  final StaticGrpcProtocol grpcProtocol = grpc(channelBuilder);

  Iterator<Map<String, Object>> droneIds() {
    return Stream.iterate(1, n -> n + 1)
        .map(id -> Map.of("droneId", (Object) String.format("drone-%05d", id)))
        .iterator();
  }

  Iterator<Coordinates> randomDeliveryPath() {
    Coordinates start = Coordinates.random(LOCATION, START_RADIUS);
    Coordinates destination = Coordinates.random(LOCATION, DESTINATION_RADIUS);
    return Coordinates.path(start, destination, TRAVEL_DISTANCE);
  }

  GrpcCallActionBuilder<ReportLocationRequest, Empty> reportLocation =
      grpc("report location")
          .rpc(DroneServiceGrpc.getReportLocationMethod())
          .payload(
              session -> {
                Coordinates coordinates = session.<Iterator<Coordinates>>get("path").next();
                return ReportLocationRequest.newBuilder()
                    .setDroneId(session.getString("droneId"))
                    .setCoordinates(coordinates.toProto())
                    .setAltitude(10)
                    .build();
              });

  ScenarioBuilder updateDrones =
      scenario("update drones")
          .feed(droneIds())
          .repeat(NUMBER_OF_DELIVERIES_PER_DRONE)
          .on(
              exec(session -> session.set("path", randomDeliveryPath()))
                  .doWhile(session -> session.<Iterator<Coordinates>>get("path").hasNext())
                  .on(pace(REPORT_EVERY).exec(reportLocation)));

  {
    setUp(updateDrones.injectOpen(rampUsers(MAX_NUMBER_OF_DRONES).during(RAMP_TIME)))
        .protocols(grpcProtocol);
  }
}
