/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.state;

public class Rewards {

  /** The state for the {@link ShoppingCart} entity. */
  public static final class State implements CborSerializable {
    private String userId;
    private int points;

    public String getUserId() {
      return userId;
    }

    public int getPoints() {
      return points;
    }
  }
}
