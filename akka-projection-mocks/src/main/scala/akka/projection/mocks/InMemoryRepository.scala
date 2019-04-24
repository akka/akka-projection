/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.mocks

import akka.Done

class InMemoryRepository[T] {

  private var list: List[T] = Nil

  def save(value: T): DBIO[Done] = {
    list = value :: list
    DBIO(Done)
  }

  def size = list.size

}
