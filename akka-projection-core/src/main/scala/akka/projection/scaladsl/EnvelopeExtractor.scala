package akka.projection.scaladsl


trait EnvelopeExtractor[Envelope, Payload, Offset] {

  def extractOffset(envelope: Envelope): Offset

  def extractPayload(envelope: Envelope): Payload

}

object EnvelopeExtractor {

  def exposeEnvelope[Envelope, Payload, Offset](extractor: EnvelopeExtractor[Envelope, Payload, Offset]): EnvelopeExtractor[Envelope, Envelope, Offset] =
    new EnvelopeExtractor[Envelope, Envelope, Offset] {
      override def extractOffset(envelope: Envelope): Offset = extractor.extractOffset(envelope)
      override def extractPayload(envelope: Envelope): Envelope = envelope
    }

}
