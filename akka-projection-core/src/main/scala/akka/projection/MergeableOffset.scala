/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

final case class MergeableOffset[Offset](val entries: Map[String, Offset])
