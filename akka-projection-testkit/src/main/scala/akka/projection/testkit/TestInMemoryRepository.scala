/*
 * Copyright (C) 2019-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit

import akka.Done
import org.slf4j.LoggerFactory

import scala.concurrent.Future

class TestInMemoryRepository[T] {

  val logger = LoggerFactory.getLogger(this.getClass)

  // FIXME: not safe, we need to harden it as test cases get more evolved
  private var internalList: List[T] = Nil

  def save(value: T): Future[Done] = {
    logger.info(s"Saving event: '$value'")
    internalList = value :: internalList
    Future.successful(Done)
  }

  def size = internalList.size

  def list = internalList.reverse

}
