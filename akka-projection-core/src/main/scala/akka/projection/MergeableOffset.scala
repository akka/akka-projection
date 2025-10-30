/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection

final case class MergeableOffset[Offset](val entries: Map[String, Offset])
