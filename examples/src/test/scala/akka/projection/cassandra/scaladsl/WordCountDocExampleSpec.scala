/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra.scaladsl

import java.util.UUID

import scala.compat.java8.FutureConverters._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.concurrent.duration._

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.projection.cassandra.ContainerSessionProvider
import akka.projection.cassandra.internal.CassandraOffsetStore
import akka.projection.testkit.scaladsl.ProjectionTestKit
import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry
import docs.cassandra.WordCountDocExample._
import org.scalatest.wordspec.AnyWordSpecLike

class WordCountDocExampleSpec
    extends ScalaTestWithActorTestKit(ContainerSessionProvider.Config)
    with AnyWordSpecLike
    with LogCapturing {

  private val session = CassandraSessionRegistry(system).sessionFor("akka.projection.cassandra")
  private implicit val ec: ExecutionContext = system.executionContext
  private val offsetStore = new CassandraOffsetStore(session)
  private val repository = new CassandraWordCountRepository(session)
  private val projectionTestKit = new ProjectionTestKit(testKit)

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    // don't use futureValue (patience) here because it can take a while to start the test container
    Await.result(ContainerSessionProvider.started, 30.seconds)

    Await.result(for {
      session <- session.underlying()
      // reason for setSchemaMetadataEnabled is that it speed up tests
      _ <- session.setSchemaMetadataEnabled(false).toScala
      // FIXME move this test to docs.cassandra package when CassandraOffsetStore.createKeyspaceAndTable is public
      _ <- offsetStore.createKeyspaceAndTable()
      _ <- repository.createKeyspaceAndTable()
      _ <- session.setSchemaMetadataEnabled(null).toScala
    } yield Done, 30.seconds)
  }

  override protected def afterAll(): Unit = {
    Await.ready(for {
      s <- session.underlying()
      // reason for setSchemaMetadataEnabled is that it speed up tests
      _ <- s.setSchemaMetadataEnabled(false).toScala
      _ <- session.executeDDL(s"DROP keyspace ${offsetStore.keyspace}")
      _ <- session.executeDDL(s"DROP keyspace ${repository.keyspace}")
      _ <- s.setSchemaMetadataEnabled(null).toScala
    } yield Done, 30.seconds)
    super.afterAll()
  }

  private def genRandomProjectionId() =
    ProjectionId(UUID.randomUUID().toString, UUID.randomUUID().toString)

  private def runAndAssert(projection: Projection[WordEnvelope]) = {
    val projectionId = projection.projectionId
    val expected = Map("abc" -> 2, "def" -> 1, "ghi" -> 1)

    projectionTestKit.run(projection) {
      withClue("check - all values words were counted") {
        val savedState = repository.loadAll(projectionId.id).futureValue
        savedState shouldBe expected
      }
    }
    withClue("check - all offsets were seen") {
      val offset = offsetStore.readOffset[Long](projectionId).futureValue.get
      offset shouldBe 4L
    }
  }

  "A handler" must {

    "be able to load initial state and manage updated state" in {
      import IllustrateStatefulHandlerLoadingInitialState._

      val projectionId = genRandomProjectionId()

      //#projection
      val projection =
        CassandraProjection
          .atLeastOnce[Long, WordEnvelope](
            projectionId,
            new WordSource,
            saveOffsetAfterEnvelopes = 1,
            saveOffsetAfterDuration = Duration.Zero,
            new WordCountHandler(projectionId, repository))
      //#projection

      runAndAssert(projection)
    }

    "be able to load state on demand and manage updated state" in {
      import IllustrateStatefulHandlerLoadingStateOnDemand._

      val projectionId = genRandomProjectionId()

      val handler = new WordCountHandler(projectionId, repository)

      val projection =
        CassandraProjection
          .atLeastOnce[Long, WordEnvelope](
            projectionId,
            new WordSource,
            saveOffsetAfterEnvelopes = 1,
            saveOffsetAfterDuration = Duration.Zero,
            handler)

      runAndAssert(projection)
    }

  }

}
