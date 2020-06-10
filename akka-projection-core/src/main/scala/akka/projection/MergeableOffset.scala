/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

trait MergeableKey {
  def surrogateKey: String
}

final case class StringKey(val surrogateKey: String) extends MergeableKey

case class MergeableOffset[Key <: MergeableKey, Offset](val entries: Map[Key, Offset])
