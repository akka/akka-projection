/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://akka.io>
 */

package docs.state

import akka.serialization.jackson.CborSerializable

object AccountEntity {
  final case class Account() extends CborSerializable
}
