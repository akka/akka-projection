/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl.transactional

import akka.Done
import akka.actor.ActorSystem
import akka.projection.scaladsl._
import akka.stream.ActorMaterializer

import scala.concurrent.Await
import scala.concurrent.duration._

object AppFakeDb extends App {

  val actorSys = ActorSystem("projections")

  import actorSys.dispatcher

  implicit val materializer = ActorMaterializer()(actorSys)

  val repository = new InMemoryRepository

  val projectionHandler = new DbProjectionHandler[String] {
    override def handleEvent(event: String): DBIO[Done] = {
      repository.save(event)
    }
  }

  val proj = Projection(
    sourceProvider = new RecordSourceProvider,
    envelopeExtractor = RecordExtractors,
    runner = new TransactionalDbRunner("fake-db"),
    handler = projectionHandler
  )

  proj.start

  Thread.sleep(3000)

  println(s"""
              | ${repository.size} events in projection
              | -----------------------------------------
            """.stripMargin)

  Await.ready(actorSys.terminate(), 3.seconds)
}
