/*
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projections

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

object FakeBroker {

  def main(args: Array[String]): Unit = {

    val actorSys = ActorSystem("projections")
    implicit val materializer = ActorMaterializer()(actorSys)
    val proj = new Projection[String] {
      override def handleEvent(msg: String) = {
        println(s"this is the element: $msg")
        Thread.sleep(100)
        Future.successful(Done)
      }
    }

    FakeBrokerProjections.atLeastOnce(proj)(Record.sourceFactory)

//    FakeBrokerProjections.atMostOnce(proj)(Record.sourceFactory)

    Thread.sleep(10000)
    println("finished")
    Await.ready( actorSys.terminate(), 5.seconds)
  }

}


object FakeBrokerProjections
  extends AtLeastOnce[Record, String]
    with AtMostOnce[Record, String] {


  override def atMostOnceCommitStrategy: AatMostOnceCommitStrategy[Record] =
    new AatMostOnceCommitStrategy[Record] {
      override def saveOffset(record: Record): Future[Done] = {
        println(s"saving offset before projection: $record")
        Future.successful(Done)
      }
    }

  override def afterStrategy: AtLeastOnceCommitStrategy[Record] =
    new AtLeastOnceCommitStrategy[Record] {
      override def saveOffset(record: Record): Future[Done] = {
        println(s"saving offset after projection: $record")
        Future.successful(Done)
      }
    }

  override def extractElement(envelope: Record): String = envelope.message
}
