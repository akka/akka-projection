/*
 * Copyright (C) 2020-2024 Lightbend Inc. <https://www.lightbend.com>
 */

//#guideProjectionRepo
package docs.guide

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.stream.alpakka.cassandra.scaladsl.CassandraSession

trait ItemPopularityProjectionRepository {

  def update(itemId: String, delta: Int): Future[Done]
  def getItem(itemId: String): Future[Option[Long]]
}

object ItemPopularityProjectionRepositoryImpl {
  val Keyspace = "akka_projection"
  val PopularityTable = "item_popularity"
}

class ItemPopularityProjectionRepositoryImpl(session: CassandraSession)(implicit val ec: ExecutionContext)
    extends ItemPopularityProjectionRepository {
  import ItemPopularityProjectionRepositoryImpl._

  override def update(itemId: String, delta: Int): Future[Done] = {
    session.executeWrite(
      s"UPDATE $Keyspace.$PopularityTable SET count = count + ? WHERE item_id = ?",
      java.lang.Long.valueOf(delta),
      itemId)
  }

  override def getItem(itemId: String): Future[Option[Long]] = {
    session
      .selectOne(s"SELECT item_id, count FROM $Keyspace.$PopularityTable WHERE item_id = ?", itemId)
      .map(opt => opt.map(row => row.getLong("count").longValue()))
  }
}
//#guideProjectionRepo
