package central.deliveries;

import akka.Done;
import akka.actor.typed.ActorSystem;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.grpc.GrpcServiceException;
import central.Coordinates;
import central.DeliveriesSettings;
import central.deliveries.proto.*;
import io.grpc.Status;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RestaurantDeliveriesServiceImpl implements RestaurantDeliveriesService {

  private static final Logger logger = LoggerFactory.getLogger(RestaurantDeliveries.class);

  private final ActorSystem<?> system;
  private final DeliveriesSettings settings;
  private final ClusterSharding sharding;

  public RestaurantDeliveriesServiceImpl(ActorSystem<?> system, DeliveriesSettings settings) {
    this.system = system;
    this.settings = settings;
    this.sharding = ClusterSharding.get(system);
  }

  @Override
  public CompletionStage<RegisterRestaurantResponse> setUpRestaurant(SetUpRestaurantRequest in) {
    logger.info(
        "Set up restaurant {}, coordinates {}-{}, location [{}]",
        in.getRestaurantId(),
        in.getCoordinates().getLatitude(),
        in.getCoordinates().getLongitude(),
        in.getLocalControlLocationId());

    if (!settings.locationIds.contains(in.getLocalControlLocationId())) {
      throw new GrpcServiceException(
          Status.INVALID_ARGUMENT.withDescription(
              "The local control location id "
                  + in.getLocalControlLocationId()
                  + " is not known to the service"));
    }

    var entityRef = sharding.entityRefFor(RestaurantDeliveries.ENTITY_KEY, in.getRestaurantId());

    var coordinates = Coordinates.fromProto(in.getCoordinates());
    CompletionStage<Done> reply =
        entityRef.askWithStatus(
            replyTo ->
                new RestaurantDeliveries.SetUpRestaurant(
                    in.getLocalControlLocationId(), coordinates, replyTo),
            settings.restaurantDeliveriesAskTimeout);

    return reply.handle(
        (done, error) -> {
          if (error != null) {
            throw new GrpcServiceException(Status.INTERNAL.withDescription(error.getMessage()));
          } else {

            return RegisterRestaurantResponse.getDefaultInstance();
          }
        });
  }

  @Override
  public CompletionStage<RegisterDeliveryResponse> registerDelivery(RegisterDeliveryRequest in) {
    logger.info(
        "Register delivery for restaurant {}, delivery id {}, destination {},{}",
        in.getRestaurantId(),
        in.getDeliveryId(),
        in.getCoordinates().getLatitude(),
        in.getCoordinates().getLongitude());

    var entityRef = sharding.entityRefFor(RestaurantDeliveries.ENTITY_KEY, in.getRestaurantId());

    var destination = Coordinates.fromProto(in.getCoordinates());

    CompletionStage<Done> reply =
        entityRef.askWithStatus(
            replyTo ->
                new RestaurantDeliveries.RegisterDelivery(in.getDeliveryId(), destination, replyTo),
            settings.restaurantDeliveriesAskTimeout);

    return reply.handle(
        (done, error) -> {
          if (error != null) {
            throw new GrpcServiceException(Status.INTERNAL.withDescription(error.getMessage()));
          } else {
            return RegisterDeliveryResponse.getDefaultInstance();
          }
        });
  }
}
