/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.jpa;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.persistence.cassandra.query.javadsl.CassandraReadJournal;
import akka.persistence.query.Offset;
import akka.projection.Projection;
import akka.projection.ProjectionId;
import akka.projection.eventsourced.EventEnvelope;
import akka.projection.eventsourced.javadsl.EventSourcedProvider;
import akka.projection.javadsl.SourceProvider;
import akka.projection.jdbc.javadsl.JdbcHandler;
import akka.projection.jdbc.javadsl.JdbcProjection;
import jdocs.eventsourced.ShoppingCart;

public class JdbcProjectionWithHibernateDocExample {

    static void  illustrateHibernate() {

        ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");

        class HibernateHandler extends JdbcHandler<EventEnvelope<ShoppingCart.Event>, HibernateSessionProvider.HibernateJdbcSession> {
            @Override
            public void process(HibernateSessionProvider.HibernateJdbcSession session, EventEnvelope<ShoppingCart.Event> eventEventEnvelope) {
                System.out.println("Hold my beer while I...");
            }

        }

        //#sourceProvider
        SourceProvider<Offset, EventEnvelope<ShoppingCart.Event>> sourceProvider =
                EventSourcedProvider.eventsByTag(system, CassandraReadJournal.Identifier(), "carts-1");
        //#sourceProvider


        HibernateSessionProvider sessionProvider = new HibernateSessionProvider();
        Projection<EventEnvelope<ShoppingCart.Event>> projection =
                JdbcProjection.exactlyOnce(
                        ProjectionId.of("shopping-carts", "carts-1"),
                        sourceProvider,
                        sessionProvider::newInstance,
                        new HibernateHandler(),
                        system
                );

    }
}
