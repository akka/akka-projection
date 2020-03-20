package akka.projection.scaladsl.jdbc

import akka.{Done, NotUsed}
import akka.projection.scaladsl.Projection
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{KillSwitch, KillSwitches, Materializer}

import scala.concurrent.{ExecutionContext, Future}

object JdbcProjection {

  def transactional[E, O](
      sourceProvider: Option[O] => Source[E, _],
      offsetStore: JdbcOffsetStore[O],
      offsetExtractor: E => O)(handler: (Connection, E) => Unit): Projection =
    new TransactionalProjection(sourceProvider, offsetStore, offsetExtractor, handler)

}

class TransactionalProjection[E, O](
    sourceProvider: Option[O] => Source[E, _],
    offsetStore: JdbcOffsetStore[O],
    offsetExtractor: E => O,
    handler: (Connection, E) => Unit)
    extends Projection {

  private var shutdown: Option[KillSwitch] = None

  private def getConnection: Connection = Connection

  private def inTransaction[T](txBlock: Connection => Unit): Future[Done] = {
    // do all the necessary to get a connection,
    // use it in a transaction and close it
    // and run `txBlock` in a dedicated dispatcher

    txBlock(getConnection)

    Future.successful(Done)
  }

  override def start()(implicit ex: ExecutionContext, materializer: Materializer): Unit = {

    // read latest offset
    val offset = offsetStore.readOffset(getConnection)

    val killSwitch =
      Source
        .future(offset.map(sourceProvider))
        .flatMapConcat[E, Any](identity)
        .mapAsync(1) { envelope =>
          inTransaction { connec =>
            offsetStore.saveOffset(connec, offsetExtractor(envelope))
            handler(connec, envelope)
          }
        }
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.ignore)(Keep.left)
        .run()

    shutdown = Some(killSwitch)
  }

  override def stop()(implicit ex: ExecutionContext): Future[Done] = {
    shutdown.foreach(_.shutdown())
    Future.successful(Done)
  }
}
