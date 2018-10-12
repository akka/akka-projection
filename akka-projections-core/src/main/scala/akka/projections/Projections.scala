/*
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projections

import akka.Done
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}

import scala.concurrent.{ExecutionContext, Future}

object Projections {

  def apply[E](projection: Projection[E])(sourceFactory: () ⇒ Source[E, _])(implicit ex: ExecutionContext, materializer: Materializer): Unit =
    apply(projection, InProjection)(sourceFactory)

  def apply[E](projection: Projection[E], commitStrategy: OffsetCommitStrategy[E])(sourceFactory: () ⇒ Source[E, _])(implicit ex: ExecutionContext, materializer: Materializer): Unit = {

    val src =
      commitStrategy match {
        case InProjection =>
          sourceFactory().mapAsync(1) { env =>
            projection.onEvent(env)
          }

        case before: AatMostOnceCommitStrategy[E] =>
          sourceFactory()
            .mapAsync(1) { e =>
              for {
                _ <- before.saveOffset(e)
                res <- projection.onEvent(e)
              } yield res
            }

        case after: AtLeastOnceCommitStrategy[E] =>
          sourceFactory()
            .mapAsync(1) { e =>
              for {
                _ <- projection.onEvent(e)
                res <-  after.saveOffset(e).map(_ => Done)
              } yield res
            }
      }

    src.runWith(Sink.ignore)
  }


  def withOffset[Envelope, Element](projection: Projection[Element], extract: Envelope => Element): Projection[Envelope] =
    new Projection[Envelope] {
      override def handleEvent(e: Envelope) =
        projection.handleEvent(extract(e))

      override def onFailure(envelope: Envelope, throwable: Throwable): Future[Done] =
        projection.onFailure(extract(envelope), throwable)
    }
}

trait AtLeastOnce[Envelope, Element] {

  def afterStrategy: AtLeastOnceCommitStrategy[Envelope]
  def extractElement(envelope: Envelope): Element

  def atLeastOnce(projection: Projection[Element])(sourceFactory: () ⇒ Source[Envelope, _])(implicit ex: ExecutionContext, materializer: Materializer): Unit = {
    val withOffset = Projections.withOffset(projection, extractElement)
    Projections(withOffset, afterStrategy)(sourceFactory)
  }
}


trait AtMostOnce[Envelope, Element] {

  def atMostOnceCommitStrategy: AatMostOnceCommitStrategy[Envelope]

  def extractElement(el: Envelope): Element
  def atMostOnce(projection: Projection[Element])(sourceFactory: () ⇒ Source[Envelope, _])(implicit ex: ExecutionContext, materializer: Materializer): Unit = {
    val withOffset = Projections.withOffset(projection, extractElement)
    Projections(withOffset, atMostOnceCommitStrategy)(sourceFactory)
  }
}

