/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.jdbc;

// #hibernate-factory-imports
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

// #hibernate-factory-imports

// #hibernate-factory
public class HibernateSessionFactory {
  private final EntityManagerFactory entityManagerFactory;

  public HibernateSessionFactory() {
    this.entityManagerFactory = Persistence.createEntityManagerFactory("akka-projection-hibernate");
  }

  public HibernateJdbcSession newInstance() {
    return new HibernateJdbcSession(entityManagerFactory.createEntityManager());
  }
}
// #hibernate-factory
