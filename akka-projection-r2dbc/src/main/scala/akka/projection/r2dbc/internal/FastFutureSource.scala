/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.r2dbc.internal

import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.scaladsl.Source

import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

/**
 * INTERNAL API
 */
@InternalApi
private[internal] object FastFutureSource {
  def fastSourceFuture[T](future: Future[T]): Source[T, NotUsed] =
    future.value match {
      case Some(Success(validation)) => Source.single(validation)
      case Some(Failure(error))      => Source.failed(error)
      case _                         => Source.future(future)
    }

  def fastFutureSource[T](futureSource: Future[Source[T, _]]): Source[T, NotUsed] =
    futureSource.value match {
      case Some(Success(source)) => source.mapMaterializedValue(_ => NotUsed)
      case Some(Failure(error))  => Source.failed(error)
      case _                     => Source.futureSource(futureSource).mapMaterializedValue(_ => NotUsed)
    }
}
