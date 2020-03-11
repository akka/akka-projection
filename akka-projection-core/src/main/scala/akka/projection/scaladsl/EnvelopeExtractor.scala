package akka.projection.scaladsl

trait EnvelopeExtractor[Envelope, Payload, OffsetType] {

  def extractOffset(envelope: Envelope): Offset[OffsetType]

  def extractPayload(envelope: Envelope): Payload

}

object EnvelopeExtractor {

  def exposeEnvelope[Envelope, Payload, OffsetType](
      extractor: EnvelopeExtractor[Envelope, Payload, OffsetType]
  ): EnvelopeExtractor[Envelope, Envelope, OffsetType] =
    new EnvelopeExtractor[Envelope, Envelope, OffsetType] {
      override def extractOffset(envelope: Envelope): Offset[OffsetType] = extractor.extractOffset(envelope)
      override def extractPayload(envelope: Envelope): Envelope = envelope
    }

}
