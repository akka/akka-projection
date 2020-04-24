/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.kafka

import akka.persistence.query.Offset

final class MessageEnvelope[CommittableMessage[K, V]](val offset: Offset) {}
