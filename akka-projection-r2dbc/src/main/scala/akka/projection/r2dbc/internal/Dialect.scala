/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc.internal

import akka.annotation.InternalApi
import akka.projection.r2dbc.R2dbcProjectionSettings

/**
 * INTERNAL API
 */
@InternalApi
private[projection] trait Dialect {
  def createOffsetStoreDao(r2dbcProjectionSettings: R2dbcProjectionSettings): OffsetStoreDao
}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] object PostgresDialect extends Dialect {
  def createOffsetStoreDao(r2dbcProjectionSettings: R2dbcProjectionSettings): OffsetStoreDao =
    new PostgresOffsetStoreDao
}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] object YugabyteDialect extends Dialect {
  def createOffsetStoreDao(r2dbcProjectionSettings: R2dbcProjectionSettings): OffsetStoreDao =
    new PostgresOffsetStoreDao
}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] object H2Dialect extends Dialect {
  def createOffsetStoreDao(r2dbcProjectionSettings: R2dbcProjectionSettings): OffsetStoreDao =
    new H2OffsetStoreDao
}
