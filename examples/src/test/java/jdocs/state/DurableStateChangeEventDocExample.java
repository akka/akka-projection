/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://akka.io>
 */

package jdocs.state;

import akka.Done;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.persistence.typed.PersistenceId;
// #changeEventHandler
import akka.persistence.typed.state.javadsl.ChangeEventHandler;
import akka.persistence.typed.state.javadsl.CommandHandlerWithReply;
import akka.persistence.typed.state.javadsl.CommandHandlerWithReplyBuilder;
import akka.persistence.typed.state.javadsl.DurableStateBehaviorWithEnforcedReplies;
import akka.persistence.typed.state.javadsl.ReplyEffect;

// #changeEventHandler
import akka.serialization.jackson.CborSerializable;
import com.fasterxml.jackson.annotation.JsonCreator;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class DurableStateChangeEventDocExample {
  static
  // #changeEventHandler
  public class ShoppingCart
      extends DurableStateBehaviorWithEnforcedReplies<ShoppingCart.Command, ShoppingCart.State> {
    // #changeEventHandler

    public final static class State implements CborSerializable {
      private Map<String, Integer> items = new HashMap<>();

      public boolean isEmpty() {
        return items.isEmpty();
      }

      public boolean hasItem(String itemId) {
        return items.containsKey(itemId);
      }

      public State updateItem(String itemId, int quantity) {
        if (quantity == 0) {
          items.remove(itemId);
        } else {
          items.put(itemId, quantity);
        }
        return this;
      }

      public State removeItem(String itemId) {
        items.remove(itemId);
        return this;
      }

    }

    public interface Command extends CborSerializable {
    }

    public static class AddItem implements Command {
      public final String itemId;
      public final int quantity;
      public final ActorRef<Done> replyTo;

      public AddItem(String itemId, int quantity, ActorRef<Done> replyTo) {
        this.itemId = itemId;
        this.quantity = quantity;
        this.replyTo = replyTo;
      }
    }

    public static class RemoveItem implements Command {
      public final String itemId;
      public final ActorRef<Done> replyTo;

      public RemoveItem(String itemId, ActorRef<Done> replyTo) {
        this.itemId = itemId;
        this.replyTo = replyTo;
      }
    }

    public static class DiscardCart implements Command {
      public final ActorRef<Done> replyTo;

      @JsonCreator
      public DiscardCart(ActorRef<Done> replyTo) {
        this.replyTo = replyTo;
      }
    }

    public interface ChangeEvent extends CborSerializable {
    }

    public static final class ItemAdded implements ChangeEvent {
      public final String itemId;
      public final int quantity;

      public ItemAdded(String itemId, int quantity) {
        this.itemId = itemId;
        this.quantity = quantity;
      }
    }

    public static final class ItemRemoved implements ChangeEvent {
      public final String itemId;

      @JsonCreator
      public ItemRemoved(String itemId) {
        this.itemId = itemId;
      }
    }

    public static final class CartRemoved implements ChangeEvent {
    }

    public static EntityTypeKey<Command> ENTITY_TYPE_KEY =
        EntityTypeKey.create(Command.class, "ShoppingCart");

    public static Behavior<Command> create(String cartId) {
      return new ShoppingCart(cartId);
    }

    private final String cartId;

    private ShoppingCart(String cartId) {
      super(
          PersistenceId.of(ENTITY_TYPE_KEY.name(), cartId),
          SupervisorStrategy.restartWithBackoff(Duration.ofMillis(200), Duration.ofSeconds(5), 0.1));
      this.cartId = cartId;
    }

    @Override
    public State emptyState() {
      return new State();
    }

    // #changeEventHandler

    @Override
    public ChangeEventHandler<Command, State, ChangeEvent> changeEventHandler() {
      return new ChangeEventHandler<Command, State, ChangeEvent>() {
        @Override
        public ChangeEvent updateHandler(State previousState, State newState, Command command) {
          if (command instanceof AddItem) {
            AddItem addItem = (AddItem) command;
            return new ItemAdded(addItem.itemId, addItem.quantity);
          } else if (command instanceof RemoveItem) {
            RemoveItem removeItem = (RemoveItem) command;
            return new ItemRemoved(removeItem.itemId);
          } else {
            throw new IllegalStateException("Unexpected command " + command.getClass());
          }
        }

        @Override
        public ChangeEvent deleteHandler(State previousState, Command command) {
          return new CartRemoved();
        }
      };
    }

    @Override
    public CommandHandlerWithReply<Command, State> commandHandler() {
      CommandHandlerWithReplyBuilder<Command, State> b = newCommandHandlerWithReplyBuilder();

      b.forAnyState()
          .onCommand(AddItem.class, this::onAddItem)
          .onCommand(RemoveItem.class, this::onRemoveItem)
          .onCommand(DiscardCart.class, this::onDiscardCart);

      return b.build();
    }

    public ReplyEffect<State> onAddItem(State state, AddItem cmd) {
      return Effect()
          .persist(state.updateItem(cmd.itemId, cmd.quantity))
          .thenReply(cmd.replyTo, updatedCart -> Done.getInstance());
    }

    public ReplyEffect<State> onRemoveItem(State state, RemoveItem cmd) {
      return Effect()
          .persist(state.removeItem(cmd.itemId))
          .thenReply(cmd.replyTo, updatedCart -> Done.getInstance());
    }

    public ReplyEffect<State> onDiscardCart(State state, DiscardCart cmd) {
      return Effect()
          .delete()
          .thenReply(cmd.replyTo, updatedCart -> Done.getInstance());
    }

  }
  // #changeEventHandler
}
