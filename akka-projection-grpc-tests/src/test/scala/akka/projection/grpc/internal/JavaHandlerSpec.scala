/*
 * Copyright (C) 2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.internal

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.Future

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.grpc.javadsl.ServiceHandler
import akka.http.javadsl.model.HttpRequest
import akka.http.javadsl.model.HttpResponse
import akka.http.javadsl.model.StatusCodes
import akka.http.scaladsl.model.{ HttpRequest => ScalaRequest }
import akka.http.scaladsl.model.{ HttpResponse => ScalaResponse }
import akka.http.scaladsl.model.HttpEntity
import akka.japi.function.{ Function => JapiFunction }
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class JavaHandlerSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  private val testKit = ActorTestKit("JavaHandlerSpec")
  private implicit val system: akka.actor.typed.ActorSystem[_] = testKit.system

  override protected def afterAll(): Unit = testKit.shutdownTestKit()

  private val noMatch: PartialFunction[ScalaRequest, Future[ScalaResponse]] = PartialFunction.empty

  private val matchPing: PartialFunction[ScalaRequest, Future[ScalaResponse]] = {
    case req if req.uri.path.toString == "/ping" => Future.successful(ScalaResponse())
  }

  private def requestWithTrackedEntity(path: String): (HttpRequest, AtomicBoolean) = {
    val materialized = new AtomicBoolean(false)
    val source = Source.single(ByteString("data")).mapMaterializedValue { _ =>
      materialized.set(true)
    }
    val request = ScalaRequest(
      uri = path,
      entity = HttpEntity(akka.http.scaladsl.model.ContentTypes.`application/octet-stream`, source))
    (request.asInstanceOf[HttpRequest], materialized)
  }

  private def get(f: JapiFunction[HttpRequest, CompletionStage[HttpResponse]], req: HttpRequest) =
    f(req).toCompletableFuture.get(5, TimeUnit.SECONDS)

  "JavaHandler" must {

    "respond 404 without touching the entity when nothing matches" in {
      val (request, materialized) = requestWithTrackedEntity("/other")
      get(JavaHandler(noMatch), request).status shouldBe StatusCodes.NOT_FOUND
      materialized.get shouldBe false
    }

    "respond using the handler when it matches" in {
      val (request, _) = requestWithTrackedEntity("/ping")
      get(JavaHandler(matchPing), request).status shouldBe StatusCodes.OK
    }

    "leave the entity readable for the next handler in a chain" in {
      val (request, materialized) = requestWithTrackedEntity("/read")
      val reader: JapiFunction[HttpRequest, CompletionStage[HttpResponse]] = { req =>
        val scalaReq = req.asInstanceOf[ScalaRequest]
        if (scalaReq.uri.path.toString == "/read")
          scalaReq.entity.dataBytes
            .runFold(0)(_ + _.size)
            .map(_ => ScalaResponse().asInstanceOf[HttpResponse])(system.executionContext)
            .asJava
        else CompletableFuture.completedFuture(HttpResponse.create().withStatus(StatusCodes.NOT_FOUND))
      }
      val chain = ServiceHandler.concatOrNotFound(system, JavaHandler(noMatch), reader)
      get(chain, request).status shouldBe StatusCodes.OK
      materialized.get shouldBe true
    }
  }

  private implicit class FutureOps[T](f: Future[T]) {
    def asJava: CompletionStage[T] = scala.jdk.FutureConverters.FutureOps(f).asJava
  }
}
