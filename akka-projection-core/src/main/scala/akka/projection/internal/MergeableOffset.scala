/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import akka.annotation.InternalApi

@InternalApi
final case class MergeableOffset[Offset](entries: Map[String, Offset])
