/*
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projections

import akka.actor.ActorSystem
import akka.projections.DBProjections.DBProjection
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.control.NonFatal

object FakeDB {

  def main(args: Array[String]): Unit = {

    val actorSys = ActorSystem("projections")
    import actorSys.dispatcher
    implicit val materializer = ActorMaterializer()(actorSys)
    val proj = new DBProjection[String] {
      override def handleEvent(msg: String) = {
        println(s"this is the element in DBIO: $msg")
        Thread.sleep(100)
        DBIO(Done)
      }

      override def onFailure(element: String, throwable: Throwable): DBIO[Done] = throw throwable
    }

    val dbProjection = new DBProjections[Record, String](rec => rec.message)
    dbProjection(proj)(Record.sourceFactory)

    Thread.sleep(10000)
    println("finished")
    Await.ready( actorSys.terminate(), 5.seconds)
  }

}

case class DBIO[+T](data: T)

class DBProjections[Envelope, Element](extract: Envelope => Element) {

  def apply(projection: DBProjection[Element])(sourceFactory: () ⇒ Source[Envelope, _])(implicit exec: ExecutionContext, materializer: Materializer): Unit = {
    val proj = new Projection[Envelope]{
      override def handleEvent(envelope: Envelope): Future[Done] = {
        Future.successful(projection.handleEvent(extract(envelope)).data).map(_ => Done)
      }

      override def onFailure(envelope: Envelope, throwable: Throwable): Future[Done] = {
        Future { projection.onFailure(extract(envelope), throwable) }.map(_ => Done)
      }
    }

    Projections(proj)(sourceFactory)
  }

}

object DBProjections {

  trait DBProjection[E] {

    def handleEvent(element: E): DBIO[Done]

    def onFailure(element: E, throwable: Throwable): DBIO[Done]

    final def onEvent(element: E)(implicit ex: ExecutionContext): DBIO[Done] = {
      handleEvent(element)
    }

  }
}