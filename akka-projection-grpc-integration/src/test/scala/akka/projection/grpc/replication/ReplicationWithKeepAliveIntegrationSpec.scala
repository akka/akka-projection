/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.replication

import akka.projection.grpc.KeepAliveTestConfig
import akka.projection.grpc.TestContainerConf

/**
 * Fully connected replication with keep alives flowing between all replicas.
 */
class ReplicationWithKeepAliveIntegrationSpec
    extends ReplicationIntegrationSpec(new TestContainerConf(KeepAliveTestConfig.config))
