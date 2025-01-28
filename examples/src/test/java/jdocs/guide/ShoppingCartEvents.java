/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

// #guideEvents
package jdocs.guide;

import akka.serialization.jackson.CborSerializable;

import java.time.Instant;

public class ShoppingCartEvents {
  public interface Event extends CborSerializable {
    String getCartId();
  }

  public interface ItemEvent extends Event {
    String getItemId();
  }

  public static final class ItemAdded implements ItemEvent {
    public final String cartId;
    public final String itemId;
    public final int quantity;

    public ItemAdded(String cartId, String itemId, int quantity) {
      this.cartId = cartId;
      this.itemId = itemId;
      this.quantity = quantity;
    }

    public String getCartId() {
      return this.cartId;
    }

    public String getItemId() {
      return this.itemId;
    }
  }

  public static final class ItemRemoved implements ItemEvent {
    public final String cartId;
    public final String itemId;
    public final int oldQuantity;

    public ItemRemoved(String cartId, String itemId, int oldQuantity) {
      this.cartId = cartId;
      this.itemId = itemId;
      this.oldQuantity = oldQuantity;
    }

    public String getCartId() {
      return this.cartId;
    }

    public String getItemId() {
      return this.itemId;
    }
  }

  public static final class ItemQuantityAdjusted implements ItemEvent {
    public final String cartId;
    public final String itemId;
    public final int newQuantity;
    public final int oldQuantity;

    public ItemQuantityAdjusted(String cartId, String itemId, int newQuantity, int oldQuantity) {
      this.cartId = cartId;
      this.itemId = itemId;
      this.newQuantity = newQuantity;
      this.oldQuantity = oldQuantity;
    }

    public String getCartId() {
      return this.cartId;
    }

    public String getItemId() {
      return this.itemId;
    }
  }

  public static final class CheckedOut implements Event {
    public final String cartId;
    public final Instant eventTime;

    public CheckedOut(String cartId, Instant eventTime) {
      this.cartId = cartId;
      this.eventTime = eventTime;
    }

    public String getCartId() {
      return this.cartId;
    }
  }
}
// #guideEvents
