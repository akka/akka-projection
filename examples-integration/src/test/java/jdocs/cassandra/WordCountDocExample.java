/*
 * Copyright (C) 2020-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.cassandra;

// #StatefulHandler-imports
import akka.actor.typed.ActorSystem;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.javadsl.StashBuffer;
import akka.projection.cassandra.CassandraProjectionTest;
import akka.projection.javadsl.ActorHandler;
import akka.projection.javadsl.StatefulHandler;

// #StatefulHandler-imports

// #ActorHandler-imports
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;

// #ActorHandler-imports

import java.time.Duration;
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
  // #todo
  // TDOO
  // #todo

  // #envelope
  public class WordEnvelope {
    public final Long offset;
    public final String word;

    public WordEnvelope(Long offset, String word) {
      this.offset = offset;
      this.word = word;
    }
  }

  // #envelope

  // #repository
  public interface WordCountRepository {
    CompletionStage<Integer> load(String id, String word);

    CompletionStage<Map<String, Integer>> loadAll(String id);

    CompletionStage<Done> save(String id, String word, int count);
  }
  // #repository

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
      return session
          .selectOne("SELECT count FROM " + keyspaceTable + " WHERE id = ? and word = ?", id, word)
          .thenApply(
              maybeRow -> {
                if (maybeRow.isPresent()) return maybeRow.get().getInt("count");
                else return 0;
              });
    }

    @Override
    public CompletionStage<Map<String, Integer>> loadAll(String id) {
      return session
          .selectAll("SELECT word, count FROM " + keyspaceTable + " WHERE id = ?", id)
          .thenApply(
              rows -> {
                return rows.stream()
                    .collect(
                        Collectors.toMap(row -> row.getString("word"), row -> row.getInt("count")));
              });
    }

    @Override
    public CompletionStage<Done> save(String id, String word, int count) {
      return session.executeWrite(
          "INSERT INTO " + keyspaceTable + " (id, word, count) VALUES (?, ?, ?)", id, word, count);
    }

    public CompletionStage<Done> createKeyspaceAndTable() {
      return session
          .executeDDL(
              "CREATE KEYSPACE IF NOT EXISTS "
                  + keyspace
                  + " WITH REPLICATION = { 'class' : 'SimpleStrategy','replication_factor':1 }")
          .thenCompose(
              done ->
                  session.executeDDL(
                      "CREATE TABLE IF NOT EXISTS "
                          + keyspaceTable
                          + " (\n"
                          + "  id text, \n"
                          + "  word text, \n"
                          + "  count int, \n"
                          + "  PRIMARY KEY (id, word)) \n"));
    }
  }

  // #sourceProvider
  class WordSource extends SourceProvider<Long, WordEnvelope> {

    private final Source<WordEnvelope, NotUsed> src =
        Source.from(
            Arrays.asList(
                new WordEnvelope(1L, "abc"),
                new WordEnvelope(2L, "def"),
                new WordEnvelope(3L, "ghi"),
                new WordEnvelope(4L, "abc")));

    @Override
    public CompletionStage<Source<WordEnvelope, NotUsed>> source(
        Supplier<CompletionStage<Optional<Long>>> offset) {
      return offset
          .get()
          .thenApply(
              o -> {
                if (o.isPresent()) return src.dropWhile(envelope -> envelope.offset <= o.get());
                else return src;
              });
    }

    @Override
    public Long extractOffset(WordEnvelope envelope) {
      return envelope.offset;
    }

    @Override
    public long extractCreationTime(WordEnvelope envelope) {
      return 0L;
    }
  }
  // #sourceProvider

  interface IllustrateVariables {
    // #mutableState
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
    // #mutableState
  }

  interface IllustrateStatefulHandlerLoadingInitialState {
    // #loadingInitialState
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
      public CompletionStage<Map<String, Integer>> process(
          Map<String, Integer> state, WordEnvelope envelope) {
        String word = envelope.word;
        int newCount = state.getOrDefault(word, 0) + 1;
        CompletionStage<Map<String, Integer>> newState =
            repository
                .save(projectionId.id(), word, newCount)
                .thenApply(
                    done -> {
                      state.put(word, newCount);
                      return state;
                    });

        return newState;
      }
    }
    // #loadingInitialState
  }

  interface IllustrateStatefulHandlerLoadingStateOnDemand {
    // #loadingOnDemand
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
      public CompletionStage<Map<String, Integer>> process(
          Map<String, Integer> state, WordEnvelope envelope) {
        String word = envelope.word;

        CompletionStage<Integer> currentCount;
        if (state.containsKey(word))
          currentCount = CompletableFuture.completedFuture(state.get(word));
        else currentCount = repository.load(projectionId.id(), word);

        CompletionStage<Map<String, Integer>> newState =
            currentCount.thenCompose(
                n -> {
                  return repository
                      .save(projectionId.id(), word, n + 1)
                      .thenApply(
                          done -> {
                            state.put(word, n + 1);
                            return state;
                          });
                });

        return newState;
      }
    }
    // #loadingOnDemand
  }

  interface IllstrateActorLoadingInitialState {

    // #actorHandler
    class WordCountActorHandler extends ActorHandler<WordEnvelope, WordCountProcessor.Command> {
      private final ActorSystem<?> system;
      private final Duration askTimeout = Duration.ofSeconds(5);

      WordCountActorHandler(Behavior<WordCountProcessor.Command> behavior, ActorSystem<?> system) {
        super(behavior);
        this.system = system;
      }

      @Override
      public CompletionStage<Done> process(
          ActorRef<WordCountProcessor.Command> actor, WordEnvelope envelope) {
        CompletionStage<WordCountProcessor.Result> result =
            AskPattern.ask(
                actor,
                (ActorRef<WordCountProcessor.Result> replyTo) ->
                    new WordCountProcessor.Handle(envelope, replyTo),
                askTimeout,
                system.scheduler());

        return result.thenCompose(
            r -> {
              if (r.error.isPresent()) {
                CompletableFuture<Done> err = new CompletableFuture<>();
                err.completeExceptionally(r.error.get());
                return err;
              } else {
                return CompletableFuture.completedFuture(Done.getInstance());
              }
            });
      }
    }
    // #actorHandler

    // #behaviorLoadingInitialState
    public class WordCountProcessor {
      public interface Command {}

      public static class Handle implements Command {
        public final WordEnvelope envelope;
        public final ActorRef<Result> replyTo;

        public Handle(WordEnvelope envelope, ActorRef<Result> replyTo) {
          this.envelope = envelope;
          this.replyTo = replyTo;
        }
      }

      public static class Result {
        public final Optional<Throwable> error;

        public Result(Optional<Throwable> error) {
          this.error = error;
        }
      }

      private static class InitialState implements Command {
        final Map<String, Integer> state;

        private InitialState(Map<String, Integer> state) {
          this.state = state;
        }
      }

      private static class SaveCompleted implements Command {
        final String word;
        final Optional<Throwable> error;
        final ActorRef<Result> replyTo;

        private SaveCompleted(String word, Optional<Throwable> error, ActorRef<Result> replyTo) {
          this.word = word;
          this.error = error;
          this.replyTo = replyTo;
        }
      }

      public static Behavior<Command> create(
          ProjectionId projectionId, WordCountRepository repository) {
        return Behaviors.supervise(
                Behaviors.setup(
                    (ActorContext<Command> context) ->
                        new WordCountProcessor(projectionId, repository).init(context)))
            .onFailure(
                SupervisorStrategy.restartWithBackoff(
                    Duration.ofSeconds(1), Duration.ofSeconds(10), 0.1));
      }

      private final ProjectionId projectionId;
      private final WordCountRepository repository;

      private WordCountProcessor(ProjectionId projectionId, WordCountRepository repository) {
        this.projectionId = projectionId;
        this.repository = repository;
      }

      Behavior<Command> init(ActorContext<Command> context) {
        return Behaviors.withStash(10, buffer -> new Initializing(context, buffer));
      }

      private class Initializing extends AbstractBehavior<Command> {
        private final StashBuffer<Command> buffer;

        private Initializing(ActorContext<Command> context, StashBuffer<Command> buffer) {
          super(context);
          this.buffer = buffer;

          getContext()
              .pipeToSelf(
                  repository.loadAll(projectionId.id()),
                  (value, exc) -> {
                    if (value != null) return new InitialState(value);
                    else throw new RuntimeException("Load failed.", exc);
                  });
        }

        @Override
        public Receive<Command> createReceive() {
          return newReceiveBuilder()
              .onMessage(InitialState.class, this::onInitalState)
              .onAnyMessage(this::onOther)
              .build();
        }

        private Behavior<Command> onInitalState(InitialState initialState) {
          getContext().getLog().debug("Initial state [{}]", initialState.state);
          return buffer.unstashAll(new Active(getContext(), initialState.state));
        }

        private Behavior<Command> onOther(Command command) {
          getContext().getLog().debug("Stashed [{}]", command);
          buffer.stash(command);
          return this;
        }
      }

      private class Active extends AbstractBehavior<Command> {
        private final Map<String, Integer> state;

        public Active(ActorContext<Command> context, Map<String, Integer> state) {
          super(context);
          this.state = state;
        }

        @Override
        public Receive<Command> createReceive() {
          return newReceiveBuilder()
              .onMessage(Handle.class, this::onHandle)
              .onMessage(SaveCompleted.class, this::onSaveCompleted)
              .build();
        }

        private Behavior<Command> onHandle(Handle command) {
          String word = command.envelope.word;
          int newCount = state.getOrDefault(word, 0) + 1;
          getContext()
              .pipeToSelf(
                  repository.save(projectionId.id(), word, newCount),
                  (done, exc) ->
                      // will reply from SaveCompleted
                      new SaveCompleted(word, Optional.ofNullable(exc), command.replyTo));
          return this;
        }

        private Behavior<Command> onSaveCompleted(SaveCompleted completed) {
          completed.replyTo.tell(new Result(completed.error));
          if (completed.error.isPresent()) {
            // restart, reload state from db
            throw new RuntimeException("Save failed.", completed.error.get());
          } else {
            String word = completed.word;
            int newCount = state.getOrDefault(word, 0) + 1;
            state.put(word, newCount);
          }
          return this;
        }
      }
    }
    // #behaviorLoadingInitialState

  }

  interface IllstrateActorLoadingStateOnDemand {

    class WordCountActorHandler extends ActorHandler<WordEnvelope, WordCountProcessor.Command> {
      private final ActorSystem<?> system;
      private final Duration askTimeout = Duration.ofSeconds(5);

      WordCountActorHandler(Behavior<WordCountProcessor.Command> behavior, ActorSystem<?> system) {
        super(behavior);
        this.system = system;
      }

      @Override
      public CompletionStage<Done> process(
          ActorRef<WordCountProcessor.Command> actor, WordEnvelope envelope) {
        CompletionStage<WordCountProcessor.Result> result =
            AskPattern.ask(
                actor,
                (ActorRef<WordCountProcessor.Result> replyTo) ->
                    new WordCountProcessor.Handle(envelope, replyTo),
                askTimeout,
                system.scheduler());

        return result.thenCompose(
            r -> {
              if (r.error.isPresent()) {
                CompletableFuture<Done> err = new CompletableFuture<>();
                err.completeExceptionally(r.error.get());
                return err;
              } else {
                return CompletableFuture.completedFuture(Done.getInstance());
              }
            });
      }
    }

    // #behaviorLoadingOnDemand
    public class WordCountProcessor extends AbstractBehavior<WordCountProcessor.Command> {
      public interface Command {}

      public static class Handle implements Command {
        public final WordEnvelope envelope;
        public final ActorRef<Result> replyTo;

        public Handle(WordEnvelope envelope, ActorRef<Result> replyTo) {
          this.envelope = envelope;
          this.replyTo = replyTo;
        }
      }

      public static class Result {
        public final Optional<Throwable> error;

        public Result(Optional<Throwable> error) {
          this.error = error;
        }
      }

      private static class LoadCompleted implements Command {
        final String word;
        final Optional<Throwable> error;
        final ActorRef<Result> replyTo;

        private LoadCompleted(String word, Optional<Throwable> error, ActorRef<Result> replyTo) {
          this.word = word;
          this.error = error;
          this.replyTo = replyTo;
        }
      }

      private static class SaveCompleted implements Command {
        final String word;
        final Optional<Throwable> error;
        final ActorRef<Result> replyTo;

        private SaveCompleted(String word, Optional<Throwable> error, ActorRef<Result> replyTo) {
          this.word = word;
          this.error = error;
          this.replyTo = replyTo;
        }
      }

      public static Behavior<Command> create(
          ProjectionId projectionId, WordCountRepository repository) {
        return Behaviors.supervise(
                Behaviors.setup(
                    (ActorContext<Command> context) ->
                        new WordCountProcessor(context, projectionId, repository)))
            .onFailure(
                SupervisorStrategy.restartWithBackoff(
                    Duration.ofSeconds(1), Duration.ofSeconds(10), 0.1));
      }

      private final ProjectionId projectionId;
      private final WordCountRepository repository;
      private final Map<String, Integer> state = new HashMap<>();

      private WordCountProcessor(
          ActorContext<Command> context,
          ProjectionId projectionId,
          WordCountRepository repository) {
        super(context);
        this.projectionId = projectionId;
        this.repository = repository;
      }

      @Override
      public Receive<Command> createReceive() {
        return newReceiveBuilder()
            .onMessage(Handle.class, this::onHandle)
            .onMessage(LoadCompleted.class, this::onLoadCompleted)
            .onMessage(SaveCompleted.class, this::onSaveCompleted)
            .build();
      }

      private Behavior<Command> onHandle(Handle command) {
        String word = command.envelope.word;
        if (state.containsKey(word)) {
          int newCount = state.get(word) + 1;
          getContext()
              .pipeToSelf(
                  repository.save(projectionId.id(), word, newCount),
                  (done, exc) ->
                      // will reply from SaveCompleted
                      new SaveCompleted(word, Optional.ofNullable(exc), command.replyTo));
        } else {
          getContext()
              .pipeToSelf(
                  repository.load(projectionId.id(), word),
                  (loadResult, exc) ->
                      // will reply from LoadCompleted
                      new LoadCompleted(word, Optional.ofNullable(exc), command.replyTo));
        }
        return this;
      }

      private Behavior<Command> onLoadCompleted(LoadCompleted completed) {
        if (completed.error.isPresent()) {
          completed.replyTo.tell(new Result(completed.error));
        } else {
          String word = completed.word;
          int newCount = state.getOrDefault(word, 0) + 1;
          getContext()
              .pipeToSelf(
                  repository.save(projectionId.id(), word, newCount),
                  (done, exc) ->
                      // will reply from SaveCompleted
                      new SaveCompleted(word, Optional.ofNullable(exc), completed.replyTo));
        }
        return this;
      }

      private Behavior<Command> onSaveCompleted(SaveCompleted completed) {
        completed.replyTo.tell(new Result(completed.error));
        if (completed.error.isPresent()) {
          // remove the word from the state if the save failed, because it could have been a timeout
          // so that it was actually saved, best to reload
          state.remove(completed.word);
        } else {
          String word = completed.word;
          int newCount = state.getOrDefault(word, 0) + 1;
          state.put(word, newCount);
        }
        return this;
      }
    }
  }
  // #behaviorLoadingOnDemand

}
