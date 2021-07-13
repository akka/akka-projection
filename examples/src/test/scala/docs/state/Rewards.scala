/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.state

object Rewards {
  final case class State(userId: String, points: Int) extends CborSerializable
}
