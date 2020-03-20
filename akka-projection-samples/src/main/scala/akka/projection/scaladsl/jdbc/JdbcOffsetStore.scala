package akka.projection.scaladsl.jdbc

import akka.persistence.query.{ Offset, TimeBasedUUID }

import scala.concurrent.Future

trait JdbcOffsetStore[O] {
  def projectionId: String
  def readOffset(connection: Connection): Future[Option[O]]
  def saveOffset(connection: Connection, offset: O): Unit
}

class JdbcOffsetStoreLong(val projectionId: String) extends JdbcOffsetStore[Long] {

  private var lastOffset: Option[Long] = None

  override def readOffset(connection: Connection): Future[Option[Long]] =
    Future.successful(lastOffset)

  override def saveOffset(connection: Connection, offset: Long): Unit =
    lastOffset = Some(offset)
}

// we will need a separated module that glues together jdbc-projection with akka persistence offset
// this implementation will need to be a little bit smarter, we will need to keep track of the offset type (Sequence / TimeBasedUUID)
// and be able to ser/deser it properly. Maybe a simplified version of Manifest with payload saved as String
class JdbcOffsetStoreAkkaOffset(val projectionId: String) extends JdbcOffsetStore[Offset] {

  private var lastOffset: Option[Offset] = None

  override def readOffset(connection: Connection): Future[Option[Offset]] =
    Future.successful(lastOffset)

  override def saveOffset(connection: Connection, offset: Offset): Unit =
    lastOffset = Some(offset)
}
