package shopping.cart;

//#eventProducerService
import akka.actor.typed.ActorSystem;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.japi.function.Function;
import akka.projection.grpc.producer.javadsl.EventProducer;
import akka.projection.grpc.producer.javadsl.Transformation;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

//#eventProducerService

public class PublishEvents {

  //#eventProducerService
  public static Function<HttpRequest, CompletionStage<HttpResponse>> eventProducerService(ActorSystem<?> system) {
    Transformation transformation =
        Transformation.empty()
            .registerMapper(ShoppingCart.ItemAdded.class, event -> Optional.of(transformItemAdded(event)))
            .registerMapper(ShoppingCart.ItemQuantityAdjusted.class, event -> Optional.of(transformItemQuantityAdjusted(event)))
            .registerMapper(ShoppingCart.ItemRemoved.class, event -> Optional.of(transformItemRemoved(event)))
            .registerMapper(ShoppingCart.CheckedOut.class, event -> Optional.of(transformCheckedOut(event)));

    return EventProducer.grpcServiceHandler(system, transformation);
  }
  //#eventProducerService

  //#transformItemAdded
  private static shopping.cart.proto.ItemAdded transformItemAdded(ShoppingCart.ItemAdded itemAdded) {
    return shopping.cart.proto.ItemAdded.newBuilder()
        .setCartId(itemAdded.cartId)
        .setItemId(itemAdded.itemId)
        .setQuantity(itemAdded.quantity)
        .build();
  }
  //#transformItemAdded

  private static shopping.cart.proto.ItemQuantityAdjusted transformItemQuantityAdjusted(ShoppingCart.ItemQuantityAdjusted itemQuantityAdjusted) {
    return shopping.cart.proto.ItemQuantityAdjusted.newBuilder()
        .setCartId(itemQuantityAdjusted.cartId)
        .setItemId(itemQuantityAdjusted.itemId)
        .setQuantity(itemQuantityAdjusted.newQuantity)
        .build();
  }

  private static shopping.cart.proto.ItemRemoved transformItemRemoved(ShoppingCart.ItemRemoved itemRemoved) {
    return shopping.cart.proto.ItemRemoved.newBuilder()
        .setCartId(itemRemoved.cartId)
        .setItemId(itemRemoved.itemId)
        .build();
  }

  private static shopping.cart.proto.CheckedOut transformCheckedOut(ShoppingCart.CheckedOut checkedOut) {
    return shopping.cart.proto.CheckedOut.newBuilder()
        .setCartId(checkedOut.cartId)
        .build();
  }
}
