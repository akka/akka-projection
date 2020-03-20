/*
 * Copyright (C) 2019-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit

import akka.Done
import org.slf4j.LoggerFactory
import scala.concurrent.Future

class TestEventHandler[E](repository: TestInMemoryRepository[E], failPredicate: E => Boolean) {

  val logger = LoggerFactory.getLogger(this.getClass)

  def onEvent(event: E): Future[Done] = {
    if (failPredicate(event)) {
      logger.info(s"failing on '$event'")
      throw new RuntimeException(s"Failed on event '$event'")
    } else
      repository.save(event)
  }

}

object TestEventHandler {

  def apply[E](repository: TestInMemoryRepository[E]): TestEventHandler[E] =
    TestEventHandler.apply(repository, (_: E) => false)

  def apply[E](repository: TestInMemoryRepository[E], failPredicate: E => Boolean): TestEventHandler[E] =
    new TestEventHandler(repository, failPredicate)
}
