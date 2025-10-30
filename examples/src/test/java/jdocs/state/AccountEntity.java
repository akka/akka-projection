/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://akka.io>
 */

package jdocs.state;

import akka.serialization.jackson.CborSerializable;

public class AccountEntity {

  /** The state for the {@link ShoppingCart} entity. */
  public static final class Account implements CborSerializable {}
}
