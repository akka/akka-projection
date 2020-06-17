/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.jdbc;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.japi.function.Function;

import akka.persistence.cassandra.query.javadsl.CassandraReadJournal;
import akka.persistence.query.Offset;
import akka.projection.Projection;
import akka.projection.ProjectionId;
import akka.projection.cassandra.javadsl.CassandraProjection;
import akka.projection.eventsourced.EventEnvelope;
import akka.projection.eventsourced.javadsl.EventSourcedProvider;
import akka.projection.javadsl.SourceProvider;
import akka.projection.jdbc.javadsl.JdbcHandler;
import akka.projection.jdbc.javadsl.JdbcProjection;
import jdocs.cassandra.CassandraProjectionDocExample;
import jdocs.eventsourced.ShoppingCart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;

// #jdbc-session-imports
import akka.projection.jdbc.JdbcSession;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Connection;

// #jdbc-session-imports

import java.time.Duration;
import java.time.Instant;
import java.util.List;

class JdbcProjectionDocExample {
  // #todo
  // TODO
  // #todo

  // #repository
  class Order {
    public final String id;
    public final Instant time;

    public Order(String id, Instant time) {
      this.id = id;
      this.time = time;
    }
  }

  interface OrderRepository {
    void save(EntityManager entityManager, Order order);
  }
  // #repository

  public OrderRepository orderRepository =
      new OrderRepository() {
        @Override
        public void save(EntityManager entityManager, Order order) {}
      };

  // #jdbc-session
  class PlainJdbcSession implements JdbcSession {

    private final Connection connection;

    public PlainJdbcSession() {
      try {
        Class.forName("org.h2.Driver");
        this.connection = DriverManager.getConnection("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
        connection.setAutoCommit(false);
      } catch (ClassNotFoundException | SQLException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public <Result> Result withConnection(Function<Connection, Result> func) throws Exception {
      return func.apply(connection);
    }

    @Override
    public void commit() throws SQLException {
      connection.commit();
    }

    @Override
    public void rollback() throws SQLException {
      connection.rollback();
    }

    @Override
    public void close() throws SQLException {
      connection.close();
    }
  }
  // #jdbc-session

  // #handler
  public class ShoppingCartHandler
      extends JdbcHandler<EventEnvelope<ShoppingCart.Event>, HibernateJdbcSession> {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void process(HibernateJdbcSession session, EventEnvelope<ShoppingCart.Event> envelope)
        throws Exception {
      ShoppingCart.Event event = envelope.event();
      if (event instanceof ShoppingCart.CheckedOut) {
        ShoppingCart.CheckedOut checkedOut = (ShoppingCart.CheckedOut) event;
        logger.info(
            "Shopping cart {} was checked out at {}", checkedOut.cartId, checkedOut.eventTime);

        // pass the EntityManager created by the projection
        // to the repository in order to use the same transaction
        orderRepository.save(
            session.entityManager, new Order(checkedOut.cartId, checkedOut.eventTime));
      } else {
        logger.debug("Shopping cart {} changed by {}", event.getCartId(), event);
      }
    }
  }
  // #handler

  // #grouped-handler
  public class GroupedShoppingCartHandler
      extends JdbcHandler<List<EventEnvelope<ShoppingCart.Event>>, HibernateJdbcSession> {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void process(
        HibernateJdbcSession session, List<EventEnvelope<ShoppingCart.Event>> envelopes)
        throws Exception {
      for (EventEnvelope<ShoppingCart.Event> envelope : envelopes) {
        ShoppingCart.Event event = envelope.event();
        if (event instanceof ShoppingCart.CheckedOut) {
          ShoppingCart.CheckedOut checkedOut = (ShoppingCart.CheckedOut) event;
          logger.info(
              "Shopping cart {} was checked out at {}", checkedOut.cartId, checkedOut.eventTime);

          // pass the EntityManager created by the projection
          // to the repository in order to use the same transaction
          orderRepository.save(
              session.entityManager, new Order(checkedOut.cartId, checkedOut.eventTime));

        } else {
          logger.debug("Shopping cart {} changed by {}", event.getCartId(), event);
        }
      }
    }
  }
  // #grouped-handler

  ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");

  // #sourceProvider
  SourceProvider<Offset, EventEnvelope<ShoppingCart.Event>> sourceProvider =
      EventSourcedProvider.eventsByTag(system, CassandraReadJournal.Identifier(), "carts-1");
  // #sourceProvider

  {
    // #exactlyOnce
    final HibernateSessionFactory sessionProvider = new HibernateSessionFactory();

    Projection<EventEnvelope<ShoppingCart.Event>> projection =
        JdbcProjection.exactlyOnce(
            ProjectionId.of("shopping-carts", "carts-1"),
            sourceProvider,
            () -> sessionProvider.newInstance(),
            () -> new ShoppingCartHandler(),
            system);
    // #exactlyOnce
  }

  {
    // #atLeastOnce
    final HibernateSessionFactory sessionProvider = new HibernateSessionFactory();
    int saveOffsetAfterEnvelopes = 100;
    Duration saveOffsetAfterDuration = Duration.ofMillis(500);

    Projection<EventEnvelope<ShoppingCart.Event>> projection =
        JdbcProjection.atLeastOnce(
                ProjectionId.of("shopping-carts", "carts-1"),
                sourceProvider,
                () -> sessionProvider.newInstance(),
                () -> new ShoppingCartHandler(),
                system)
            .withSaveOffset(saveOffsetAfterEnvelopes, saveOffsetAfterDuration);
    // #atLeastOnce
  }

  {
    // #grouped
    final HibernateSessionFactory sessionProvider = new HibernateSessionFactory();
    int saveOffsetAfterEnvelopes = 100;
    Duration saveOffsetAfterDuration = Duration.ofMillis(500);

    Projection<EventEnvelope<ShoppingCart.Event>> projection =
        JdbcProjection.groupedWithin(
                ProjectionId.of("shopping-carts", "carts-1"),
                sourceProvider,
                () -> sessionProvider.newInstance(),
                () -> new GroupedShoppingCartHandler(),
                system)
            .withGroup(saveOffsetAfterEnvelopes, saveOffsetAfterDuration);
    // #grouped
  }
}
