/*
 * Copyright (C) 2020-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.state

import akka.serialization.jackson.CborSerializable

object AccountEntity {
  final case class Account() extends CborSerializable
}
