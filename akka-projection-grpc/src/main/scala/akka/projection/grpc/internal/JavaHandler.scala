/*
 * Copyright (C) 2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.internal

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.FutureConverters._

import akka.annotation.InternalApi
import akka.http.javadsl.model.HttpRequest
import akka.http.javadsl.model.HttpResponse
import akka.http.javadsl.model.StatusCodes
import akka.japi.function.{ Function => JapiFunction }

/**
 * INTERNAL API
 */
@InternalApi private[akka] object JavaHandler {

  /**
   * Responds with 404 without touching the request entity when the handler does not match, so that the
   * next handler can still read it.
   */
  def apply(
      handler: PartialFunction[akka.http.scaladsl.model.HttpRequest, Future[akka.http.scaladsl.model.HttpResponse]])
      : JapiFunction[HttpRequest, CompletionStage[HttpResponse]] = {
    val notFound =
      CompletableFuture.completedFuture[HttpResponse](HttpResponse.create().withStatus(StatusCodes.NOT_FOUND))
    (request: HttpRequest) => {
      val scalaRequest = request.asInstanceOf[akka.http.scaladsl.model.HttpRequest]
      if (handler.isDefinedAt(scalaRequest))
        handler(scalaRequest).map(_.asInstanceOf[HttpResponse])(ExecutionContext.parasitic).asJava
      else notFound
    }
  }
}
