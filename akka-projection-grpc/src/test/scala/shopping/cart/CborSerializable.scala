/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package shopping.cart

/**
 * Marker trait for serialization with Jackson CBOR.
 * Enabled in serialization.conf `akka.actor.serialization-bindings` (via application.conf).
 */
trait CborSerializable
