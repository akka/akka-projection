/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection

/**
 * When used as metadata of the EventEnvelope the offset store will allow gaps
 * in sequence numbers when validating the offset.
 */
object AllowSeqNrGapsMetadata extends AllowSeqNrGapsMetadata {

  /**
   * Java API: The singleton instance
   */
  def getInstance: AllowSeqNrGapsMetadata = AllowSeqNrGapsMetadata

}

trait AllowSeqNrGapsMetadata
