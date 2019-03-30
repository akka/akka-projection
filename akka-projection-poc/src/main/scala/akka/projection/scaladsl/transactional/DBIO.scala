/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl.transactional

import scala.concurrent.Future

/**
 * Fake DBIO 'Monad' just for the PoC
 */
case class DBIO[A](value: A) {
  def flatMap[B](f: A => DBIO[B]): DBIO[B] = f(value)
}

object Database {
  def run[A](dbio: DBIO[A]): Future[A] = Future.successful(dbio.value)
}