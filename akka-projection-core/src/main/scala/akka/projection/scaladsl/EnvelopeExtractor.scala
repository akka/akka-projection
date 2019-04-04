package akka.projection.scaladsl


trait EnvelopeExtractor[Envelope, Payload, Offset] {

  def extractOffset(envelope: Envelope): Offset

  def extractPayload(envelope: Envelope): Payload
}
