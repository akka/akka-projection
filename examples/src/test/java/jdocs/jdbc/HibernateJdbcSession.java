/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.jdbc;

import akka.japi.function.Function;
import akka.projection.jdbc.JdbcSession;
// #hibernate-session-imports
import org.hibernate.Session;
import org.hibernate.jdbc.ReturningWork;
import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import java.sql.Connection;
import java.sql.SQLException;

// #hibernate-session-imports

// #hibernate-session
public class HibernateJdbcSession implements JdbcSession {

  final EntityManager entityManager;
  private final EntityTransaction transaction;

  public HibernateJdbcSession(EntityManager entityManager) {
    this.entityManager = entityManager;
    this.transaction = this.entityManager.getTransaction();
    this.transaction.begin();
  }

  @Override
  public <Result> Result withConnection(Function<Connection, Result> func) {
    Session hibernateSession = ((Session) entityManager.getDelegate());
    return hibernateSession.doReturningWork(
        new ReturningWork<Result>() {
          @Override
          public Result execute(Connection connection) throws SQLException {
            try {
              return func.apply(connection);
            } catch (SQLException e) {
              throw e;
            } catch (Exception e) {
              throw new SQLException(e);
            }
          }
        });
  }

  @Override
  public void commit() {
    transaction.commit();
  }

  @Override
  public void rollback() {
    // propagates rollback call if transaction is active
    if (transaction.isActive()) transaction.rollback();
  }

  @Override
  public void close() {
    this.entityManager.close();
  }
}
// #hibernate-session
