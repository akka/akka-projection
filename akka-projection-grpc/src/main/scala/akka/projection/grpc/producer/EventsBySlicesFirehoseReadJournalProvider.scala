/*
 * Copyright (C)2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.producer

import akka.actor.ExtendedActorSystem
import akka.persistence.query.ReadJournalProvider
import com.typesafe.config.Config

final class EventsBySlicesFirehoseReadJournalProvider(system: ExtendedActorSystem, config: Config, cfgPath: String)
    extends ReadJournalProvider {

  private lazy val scaladslReadJournalInstance: scaladsl.EventsBySlicesFirehoseReadJournal =
    new scaladsl.EventsBySlicesFirehoseReadJournal(system, config, cfgPath)

  override def scaladslReadJournal(): scaladsl.EventsBySlicesFirehoseReadJournal = scaladslReadJournalInstance

  private lazy val javadslReadJournalInstance = new javadsl.EventsBySlicesFirehoseReadJournal(
    new scaladsl.EventsBySlicesFirehoseReadJournal(system, config, cfgPath))

  override def javadslReadJournal(): javadsl.EventsBySlicesFirehoseReadJournal = javadslReadJournalInstance
}
