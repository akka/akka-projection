/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.cassandra

import java.util.UUID

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.projection.cassandra.ContainerSessionProvider
import akka.projection.cassandra.scaladsl.CassandraProjection
import akka.projection.testkit.scaladsl.ProjectionTestKit
import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry
import docs.cassandra.WordCountDocExample._
import org.scalatest.wordspec.AnyWordSpecLike

class WordCountDocExampleSpec
    extends ScalaTestWithActorTestKit(ContainerSessionProvider.Config)
    with AnyWordSpecLike
    with LogCapturing {

  private implicit val ec: ExecutionContext = system.executionContext
  private val session = CassandraSessionRegistry(system).sessionFor("akka.projection.cassandra.session-config")
  private val repository = new CassandraWordCountRepository(session)
  private val projectionTestKit = ProjectionTestKit(system)

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    // don't use futureValue (patience) here because it can take a while to start the test container
    Await.result(ContainerSessionProvider.started, 30.seconds)

    Await.result(for {
      _ <- CassandraProjection.createTablesIfNotExists()
      _ <- repository.createKeyspaceAndTable()
    } yield Done, 30.seconds)
  }

  override protected def afterAll(): Unit = {
    Await.ready(for {
      _ <- session.executeDDL(s"DROP keyspace akka_projection.offset_store")
      _ <- session.executeDDL(s"DROP keyspace ${repository.keyspace}")
    } yield Done, 30.seconds)
    super.afterAll()
  }

  private def genRandomProjectionId() =
    ProjectionId(UUID.randomUUID().toString, UUID.randomUUID().toString)

  private def runAndAssert(projection: Projection[WordEnvelope]): Unit = {
    val projectionId = projection.projectionId
    val expected = Map("abc" -> 2, "def" -> 1, "ghi" -> 1)

    projectionTestKit.run(projection) {
      withClue("check - all values words were counted") {
        val savedState = repository.loadAll(projectionId.id).futureValue
        savedState shouldBe expected
      }
    }
  }

  "WordCount example" must {

    "be able to load initial state and manage updated state" in {
      import IllustrateStatefulHandlerLoadingInitialState._

      val projectionId = genRandomProjectionId()

      //#projection
      val projection =
        CassandraProjection
          .atLeastOnce[Long, WordEnvelope](
            projectionId,
            sourceProvider = new WordSource,
            handler = () => new WordCountHandler(projectionId, repository))
      //#projection

      runAndAssert(projection)
    }

    "be able to load state on demand and manage updated state" in {
      import IllustrateStatefulHandlerLoadingStateOnDemand._

      val projectionId = genRandomProjectionId()

      val projection =
        CassandraProjection
          .atLeastOnce[Long, WordEnvelope](
            projectionId,
            sourceProvider = new WordSource,
            handler = () => new WordCountHandler(projectionId, repository))

      runAndAssert(projection)
    }

    "have support for actor Behavior as handler - loading initial state" in {
      import IllstrateActorLoadingInitialState._

      val projectionId = genRandomProjectionId()

      //#actorHandlerProjection
      val projection =
        CassandraProjection
          .atLeastOnce[Long, WordEnvelope](
            projectionId,
            sourceProvider = new WordSource,
            handler = () => new WordCountActorHandler(WordCountProcessor(projectionId, repository)))
      //#actorHandlerProjection

      runAndAssert(projection)
    }

    "have support for actor Behavior as handler - loading state on demand" in {
      import IllstrateActorLoadingStateOnDemand._

      val projectionId = genRandomProjectionId()

      val projection =
        CassandraProjection
          .atLeastOnce[Long, WordEnvelope](
            projectionId,
            new WordSource,
            () => new WordCountActorHandler(WordCountProcessor(projectionId, repository)))

      runAndAssert(projection)
    }

  }

}
