package akka.projection.scaladsl.kafka

import akka.Done
import akka.projection.scaladsl.AsyncEventHandler
import akka.projection.testkit.{DBIO, DbEventHandler, InMemoryRepository}

import scala.concurrent.ExecutionContext

object TestEventHandler {

  def dbEventHandler(repository: InMemoryRepository[String], failPredicate: String => Boolean = _ => false): DbEventHandler[String] =
    new DbEventHandler[String] {
      override def handleEvent(event: String): DBIO[Done] = {
        if (failPredicate(event))
          throw new RuntimeException(s"Failed on event $event")
        else
          repository.save(event)
      }
    }

  def dbEventHandlerAsync(repository: InMemoryRepository[String], failPredicate: String => Boolean = _ => false)(implicit ec: ExecutionContext): AsyncEventHandler[String] =
    dbEventHandler(repository, failPredicate).asAsyncHandler
}
