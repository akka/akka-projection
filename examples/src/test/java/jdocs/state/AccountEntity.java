/*
 * Copyright (C) 2020-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.state;

import akka.serialization.jackson.CborSerializable;

public class AccountEntity {

  /** The state for the {@link ShoppingCart} entity. */
  public static final class Account implements CborSerializable {}
}
