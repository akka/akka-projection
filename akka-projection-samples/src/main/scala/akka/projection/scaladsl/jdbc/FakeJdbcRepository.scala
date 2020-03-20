package akka.projection.scaladsl.jdbc

import org.slf4j.LoggerFactory

class FakeJdbcRepository[T] {

  val logger = LoggerFactory.getLogger(this.getClass)

  // FIXME: not safe, we need to harden it as test cases get more evolved
  private var internalList: List[T] = Nil

  def save(value: T): Unit = {
    logger.info(s"Saving event: '$value'")
    internalList = value :: internalList
  }

  def size = internalList.size

  def list = internalList.reverse

}
