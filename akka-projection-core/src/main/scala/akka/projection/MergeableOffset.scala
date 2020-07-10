/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

final case class MergeableOffset[Offset](val entries: Map[String, Offset])
