package shopping.cart;

//#eventProducerService
import akka.actor.typed.ActorSystem;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.japi.function.Function;
import akka.persistence.query.typed.EventEnvelope;
import akka.persistence.typed.PersistenceId;
import akka.projection.grpc.producer.EventProducerSettings;
import akka.projection.grpc.producer.javadsl.EventProducer;
import akka.projection.grpc.producer.javadsl.EventProducerSource;
import akka.projection.grpc.producer.javadsl.Transformation;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class PublishEvents {

  public static Function<HttpRequest, CompletionStage<HttpResponse>> eventProducerService(ActorSystem<?> system) {
    Transformation transformation =
        Transformation.empty()
            .registerAsyncEnvelopeMapper(ShoppingCart.ItemUpdated.class, envelope -> CompletableFuture.completedFuture(Optional.of(transformItemQuantityAdjusted(envelope))))
            .registerAsyncEnvelopeMapper(ShoppingCart.CheckedOut.class, envelope -> CompletableFuture.completedFuture(Optional.of(transformCheckedOut(envelope))));

    //#withProducerFilter
    EventProducerSource eventProducerSource = new EventProducerSource(
        "ShoppingCart",
        "cart",
        transformation,
        EventProducerSettings.apply(system)
    //#eventProducerService
    )
    .withProducerFilter(envelope -> {
      Set<String> tags = envelope.getTags();
      return tags.contains(ShoppingCart.MEDIUM_QUANTITY_TAG) ||
          tags.contains(ShoppingCart.LARGE_QUANTITY_TAG);
    });
    //#withProducerFilter

    /* for doc snippet to render the ); at the right place
    //#eventProducerService
    );
    //#eventProducerService
    */
    //#eventProducerService

    return EventProducer.grpcServiceHandler(system, eventProducerSource);
  }
  //#eventProducerService

  //#transformItemUpdated
  private static shopping.cart.proto.ItemQuantityAdjusted transformItemQuantityAdjusted(EventEnvelope<ShoppingCart.ItemUpdated> envelope) {
    var itemUpdated = envelope.event();
    return shopping.cart.proto.ItemQuantityAdjusted.newBuilder()
        .setCartId(PersistenceId.extractEntityId(envelope.persistenceId()))
        .setItemId(itemUpdated.itemId)
        .setQuantity(itemUpdated.quantity)
        .build();
  }
  //#transformItemUpdated

  private static shopping.cart.proto.CheckedOut transformCheckedOut(EventEnvelope<ShoppingCart.CheckedOut> envelope) {
    return shopping.cart.proto.CheckedOut.newBuilder()
        .setCartId(PersistenceId.extractEntityId(envelope.persistenceId()))
        .build();
  }
//#eventProducerService
}
//#eventProducerService
