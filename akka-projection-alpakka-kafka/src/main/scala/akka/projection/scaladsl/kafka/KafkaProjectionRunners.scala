package akka.projection.scaladsl.kafka

import akka.Done
import akka.kafka.ConsumerMessage.CommittableOffset
import akka.projection.scaladsl.{OffsetStore, ProjectionRunner}

import scala.concurrent.{ExecutionContext, Future}

object KafkaProjectionRunners {

  val atLeastOnceRunner = AtLeastOnceRunner

  object AtLeastOnceRunner extends ProjectionRunner[CommittableOffset, Future[Done]] {
    override def offsetStore: OffsetStore[CommittableOffset, Future[Done]] = NoopsOffsetStore

    override def run(offset: CommittableOffset)(handler: () => Future[Done])(implicit ec: ExecutionContext): Future[Done] = {
      handler().flatMap(_ => offset.commitScaladsl())
    }
  }


  val atMostOnceRunner = AtMostOnceRunner

  object AtMostOnceRunner extends ProjectionRunner[CommittableOffset, Future[Done]] {
    override def offsetStore: OffsetStore[CommittableOffset, Future[Done]] = NoopsOffsetStore

    override def run(offset: CommittableOffset)(handler: () => Future[Done])(implicit ec: ExecutionContext): Future[Done] = {
      offset.commitScaladsl().flatMap(_ => handler())
    }
  }

  // do nothing offset store because commits are done in topic
  private object NoopsOffsetStore extends OffsetStore[CommittableOffset, Future[Done]] {
    override def readOffset(): Future[Option[CommittableOffset]] = Future.successful(None)

    override def saveOffset(offset: CommittableOffset): Future[Done] = Future.successful(Done)
  }

}
