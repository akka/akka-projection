/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl.transactional

import akka.Done

class InMemoryRepository {



  private var list: List[String] = Nil

  def save(str: String): DBIO[Done] = {
    list = str :: list
    DBIO(Done)
  }

  def size = list.size

}
