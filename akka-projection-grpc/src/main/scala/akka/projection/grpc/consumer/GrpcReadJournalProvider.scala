/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer

import akka.actor.ExtendedActorSystem
import akka.persistence.query.ReadJournalProvider
import akka.projection.grpc.internal.ProtoAnySerialization
import com.typesafe.config.Config

final class GrpcReadJournalProvider(system: ExtendedActorSystem, config: Config, cfgPath: String)
    extends ReadJournalProvider {
  override val scaladslReadJournal: scaladsl.GrpcReadJournal =
    new scaladsl.GrpcReadJournal(system, config, cfgPath)

  override val javadslReadJournal: javadsl.GrpcReadJournal =
    new javadsl.GrpcReadJournal(
      new scaladsl.GrpcReadJournal(system, config, cfgPath, ProtoAnySerialization.Prefer.Java))
}
