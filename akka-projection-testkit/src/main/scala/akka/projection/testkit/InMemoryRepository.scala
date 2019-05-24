/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit

import akka.Done

class InMemoryRepository[T] {

  private var internalList: List[T] = Nil

  def save(value: T): DBIO[Done] = {
    internalList = value :: internalList
    DBIO(Done)
  }

  def size = internalList.size

  def list = internalList.reverse

}
