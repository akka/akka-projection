/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

// #guideProjectionRepo
package jdocs.guide;

import akka.Done;
import akka.stream.alpakka.cassandra.javadsl.CassandraSession;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

interface ItemPopularityProjectionRepository {
  CompletionStage<Done> update(String itemId, int delta);

  CompletionStage<Optional<Long>> getItem(String itemId);
}

class ItemPopularityProjectionRepositoryImpl implements ItemPopularityProjectionRepository {

  public static final String Keyspace = "akka_projection";
  public static final String PopularityTable = "item_popularity";

  CassandraSession session;

  public ItemPopularityProjectionRepositoryImpl(CassandraSession session) {
    this.session = session;
  }

  @Override
  public CompletionStage<Done> update(String itemId, int delta) {
    return session.executeWrite(
        String.format(
            "UPDATE %s.%s SET count = count + ? WHERE item_id = ?", Keyspace, PopularityTable),
        (long) delta,
        itemId);
  }

  @Override
  public CompletionStage<Optional<Long>> getItem(String itemId) {
    return session
        .selectOne(
            String.format(
                "SELECT item_id, count FROM %s.%s WHERE item_id = ?", Keyspace, PopularityTable),
            itemId)
        .thenApply(opt -> opt.map(row -> row.getLong("count")));
  }
}
// #guideProjectionRepo
