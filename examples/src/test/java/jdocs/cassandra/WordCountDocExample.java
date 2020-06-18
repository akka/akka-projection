/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.cassandra;

//#StatefulHandler-imports
import akka.projection.javadsl.StatefulHandler;

//#StatefulHandler-imports

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import akka.Done;
import akka.NotUsed;
import akka.projection.ProjectionId;
import akka.projection.javadsl.Handler;
import akka.projection.javadsl.SourceProvider;
import akka.stream.alpakka.cassandra.javadsl.CassandraSession;
import akka.stream.javadsl.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface WordCountDocExample {
  //#todo
  // TDOO
  //#todo

  //#envelope
  public class WordEnvelope {
    public final Long offset;
    public final String word;

    public WordEnvelope(Long offset, String word) {
      this.offset = offset;
      this.word = word;
    }
  }

  //#envelope

  //#repository
  public interface WordCountRepository {
    CompletionStage<Integer> load(String id, String word);

    CompletionStage<Map<String, Integer>> loadAll(String id);

    CompletionStage<Done> save(String id, String word, int count);
  }
  //#repository

  public class CassandraWordCountRepository implements WordCountRepository {
    private final CassandraSession session;

    final String keyspace = "test";
    final String table = "wordcount";
    private final String keyspaceTable = keyspace + "." + table;

    public CassandraWordCountRepository(CassandraSession session) {
      this.session = session;
    }

    @Override
    public CompletionStage<Integer> load(String id, String word) {
      return session.selectOne("SELECT count FROM " + keyspaceTable + " WHERE id = ? and word = ?", id, word)
          .thenApply(maybeRow -> {
            if (maybeRow.isPresent())
              return maybeRow.get().getInt("count");
            else
              return 0;
          });
    }

    @Override
    public CompletionStage<Map<String, Integer>> loadAll(String id) {
      return session.selectAll("SELECT word, count FROM " + keyspaceTable + " WHERE id = ?", id)
          .thenApply(rows -> {
            return rows
                .stream()
                .collect(Collectors.toMap(row -> row.getString("word"), row -> row.getInt("count")));
          });
    }

    @Override
    public CompletionStage<Done> save(String id, String word, int count) {
      return session.executeWrite(
          "INSERT INTO " + keyspaceTable + " (id, word, count) VALUES (?, ?, ?)",
          id,
          word,
          count);
    }

    public CompletionStage<Done> createKeyspaceAndTable() {
      return session
          .executeDDL(
              "CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH REPLICATION = { 'class' : 'SimpleStrategy','replication_factor':1 }")
          .thenCompose(done ->
              session.executeDDL(
                  "CREATE TABLE IF NOT EXISTS " + keyspaceTable + " (\n" +
                      "  id text, \n" +
                      "  word text, \n" +
                      "  count int, \n" +
                      "  PRIMARY KEY (id, word)) \n"));
    }
  }

  //#sourceProvider
  class WordSource implements SourceProvider<Long, WordEnvelope> {

    private final Source<WordEnvelope, NotUsed> src = Source.from(
        Arrays.asList(new WordEnvelope(1L, "abc"), new WordEnvelope(2L, "def"), new WordEnvelope(3L, "ghi"), new WordEnvelope(4L, "abc")));

    @Override
    public CompletionStage<Source<WordEnvelope, ?>> source(Supplier<CompletionStage<Optional<Long>>> offset) {
      return offset.get().thenApply(o -> {
        if (o.isPresent())
          return src.dropWhile(envelope -> envelope.offset <= o.get());
        else
          return src;
      });
    }

    @Override
    public Long extractOffset(WordEnvelope envelope) {
      return envelope.offset;
    }

  }
  //#sourceProvider

  interface IllustrateVariables {
    //#mutableState
    public class WordCountHandler extends Handler<WordEnvelope> {
      private final Logger logger = LoggerFactory.getLogger(getClass());
      private final Map<String, Integer> state = new HashMap<>();


      @Override
      public CompletionStage<Done> process(WordEnvelope envelope) {
        String word = envelope.word;
        int newCount = state.getOrDefault(word, 0) + 1;
        logger.info("Word count for {} is {}", word, newCount);
        state.put(word, newCount);
        return CompletableFuture.completedFuture(Done.getInstance());
      }
    }
    //#mutableState
  }

  interface IllustrateStatefulHandlerLoadingInitialState {
    //#loadingInitialState
    public class WordCountHandler extends StatefulHandler<Map<String, Integer>, WordEnvelope> {
      private final ProjectionId projectionId;
      private final WordCountRepository repository;

      public WordCountHandler(ProjectionId projectionId, WordCountRepository repository) {
        this.projectionId = projectionId;
        this.repository = repository;
      }

      @Override
      public CompletionStage<Map<String, Integer>> initialState() {
        return repository.loadAll(projectionId.id());
      }

      @Override
      public CompletionStage<Map<String, Integer>> process(Map<String, Integer> state, WordEnvelope envelope) {
        String word = envelope.word;
        int newCount = state.getOrDefault(word, 0) + 1;
        CompletionStage<Map<String, Integer>> newState =
            repository
                .save(projectionId.id(), word, newCount)
                .thenApply(done -> {
                  state.put(word, newCount);
                  return state;
                });

        return newState;
      }
    }
    //#loadingInitialState
  }

  interface IllustrateStatefulHandlerLoadingStateOnDemand {
    //#loadingOnDemand
    public class WordCountHandler extends StatefulHandler<Map<String, Integer>, WordEnvelope> {
      private final ProjectionId projectionId;
      private final WordCountRepository repository;

      public WordCountHandler(ProjectionId projectionId, WordCountRepository repository) {
        this.projectionId = projectionId;
        this.repository = repository;
      }

      @Override
      public CompletionStage<Map<String, Integer>> initialState() {
        return CompletableFuture.completedFuture(new HashMap<>());
      }

      @Override
      public CompletionStage<Map<String, Integer>> process(Map<String, Integer> state, WordEnvelope envelope) {
        String word = envelope.word;

        CompletionStage<Integer> currentCount;
        if (state.containsKey(word))
          currentCount = CompletableFuture.completedFuture(state.get(word));
        else
          currentCount = repository.load(projectionId.id(), word);

        CompletionStage<Map<String, Integer>> newState =
            currentCount.thenCompose(n -> {
              return repository.save(projectionId.id(), word, n + 1)
                  .thenApply(done -> {
                    state.put(word, n + 1);
                    return state;
                  });
            });

        return newState;
      }
    }
    //#loadingOnDemand
  }


}
