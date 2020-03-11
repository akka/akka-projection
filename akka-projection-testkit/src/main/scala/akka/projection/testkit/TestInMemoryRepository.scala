/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit

import akka.Done
import org.slf4j.LoggerFactory

class TestInMemoryRepository[T] {

  val logger = LoggerFactory.getLogger(this.getClass)

  // FIXME: not safe, we need to harden it as test cases get more evolved
  private var internalList: List[T] = Nil

  def save(value: T): DBIO[Done] = {
    logger.info(s"Saving event: $value")
    internalList = value :: internalList
    DBIO(Done)
  }

  def size = internalList.size

  def list = internalList.reverse

}
