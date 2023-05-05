package shopping.cart;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.japi.function.Function;
import akka.management.cluster.bootstrap.ClusterBootstrap;
import akka.management.javadsl.AkkaManagement;
import akka.projection.grpc.replication.javadsl.Replication;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shopping.cart.proto.ShoppingCartService;

import java.util.concurrent.CompletionStage;

public class Main {

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "shopping-cart-service");
    try {
      init(system);
    } catch (Exception e) {
      logger.error("Terminating due to initialization failure.", e);
      system.terminate();
    }
  }

  public static void init(ActorSystem<Void> system) {
    AkkaManagement.get(system).start();
    ClusterBootstrap.get(system).start();

    // #single-service-handler
    Replication<ShoppingCart.Command> replicatedShoppingCart = ShoppingCart.init(system);
    // alternatively
    // Replication<ShoppingCart.Command> replicatedShoppingCart = ShoppingCart.initWithProducerFilter(system);
    Function<HttpRequest, CompletionStage<HttpResponse>> replicationService =
        replicatedShoppingCart.createSingleServiceHandler();
    // #single-service-handler

    Config config = system.settings().config();
    String grpcInterface = config.getString("shopping-cart-service.grpc.interface");
    int grpcPort = config.getInt("shopping-cart-service.grpc.port");
    ShoppingCartService grpcService =
        new ShoppingCartServiceImpl(system, replicatedShoppingCart.entityTypeKey());
    ShoppingCartServer.start(grpcInterface, grpcPort, system, grpcService, replicationService);
  }

}
