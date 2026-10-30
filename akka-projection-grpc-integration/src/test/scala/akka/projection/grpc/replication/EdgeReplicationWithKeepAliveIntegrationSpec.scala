/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.replication

import akka.projection.grpc.KeepAliveTestConfig
import akka.projection.grpc.TestContainerConf

/**
 * Edge replication with keep alives flowing on the pull based streams.
 */
class EdgeReplicationWithKeepAliveIntegrationSpec
    extends EdgeReplicationIntegrationSpec(new TestContainerConf(KeepAliveTestConfig.config))
