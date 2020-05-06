/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.util.Try

import akka.Done
import akka.NotUsed
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.projection.ProjectionId
import akka.projection.cassandra.internal.CassandraOffsetStore
import akka.projection.cassandra.scaladsl.CassandraProjection
import akka.projection.scaladsl.SourceProvider
import akka.projection.testkit.scaladsl.ProjectionTestKit
import akka.stream.alpakka.cassandra.scaladsl.CassandraSession
import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSource
import org.scalatest.wordspec.AnyWordSpecLike

object CassandraProjectionSpec {
  case class Envelope(id: String, offset: Long, message: String)

  def offsetExtractor(env: Envelope): Long = env.offset

  def sourceProvider(id: String): SourceProvider[Long, Envelope] = {

    val envelopes =
      List(
        Envelope(id, 1L, "abc"),
        Envelope(id, 2L, "def"),
        Envelope(id, 3L, "ghi"),
        Envelope(id, 4L, "jkl"),
        Envelope(id, 5L, "mno"),
        Envelope(id, 6L, "pqr"))

    TestSourceProvider(Source(envelopes))
  }

  case class TestSourceProvider(src: Source[Envelope, _]) extends SourceProvider[Long, Envelope] {

    override def source(offset: Option[Long]): Source[Envelope, _] = {
      offset match {
        case Some(o) => src.dropWhile(_.offset <= o)
        case _       => src
      }
    }

    override def extractOffset(env: Envelope): Long = env.offset
  }

  // test model is as simple as a text that gets other string concatenated to it
  case class ConcatStr(id: String, text: String) {
    def concat(newMsg: String): ConcatStr = copy(text = text + "|" + newMsg)
  }

  class TestRepository(session: CassandraSession)(implicit val ec: ExecutionContext) {
    val keyspace = "test"
    val table = "test_model"

    def concatToText(id: String, payload: String)(implicit ec: ExecutionContext): Future[Done.type] = {
      for {
        concatStr <- findById(id).map {
          case Some(concatStr) => concatStr.concat(payload)
          case _               => ConcatStr(id, payload)
        }
        _ <- session.executeWrite(s"INSERT INTO $keyspace.$table (id, concatenated) VALUES (?, ?)", id, concatStr.text)
      } yield Done
    }

    def findById(id: String): Future[Option[ConcatStr]] = {
      session.selectOne(s"SELECT concatenated FROM $keyspace.$table WHERE id = ?", id).map {
        case Some(row) => Some(ConcatStr(id, row.getString("concatenated")))
        case None      => None
      }
    }

    def readValue(id: String): Future[String] = {
      findById(id).map {
        case Some(concatStr) => concatStr.text
        case _               => "N/A"
      }
    }

    def createKeyspaceAndTable(): Future[Done] = {
      session
        .executeDDL(
          s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy','replication_factor':1 }")
        .flatMap(_ => session.executeDDL(s"""
        |CREATE TABLE IF NOT EXISTS $keyspace.$table (
        |  id text,
        |  concatenated text,
        |  PRIMARY KEY (id))
        """.stripMargin.trim))
    }
  }

}

class CassandraProjectionSpec
    extends ScalaTestWithActorTestKit(ContainerSessionProvider.Config)
    with AnyWordSpecLike
    with LogCapturing {

  import CassandraProjectionSpec._

  private val session = CassandraSessionRegistry(system).sessionFor("akka.projection.cassandra")
  private implicit val ec: ExecutionContext = system.executionContext
  private val offsetStore = new CassandraOffsetStore(session)
  private val repository = new TestRepository(session)
  private val projectionTestKit = new ProjectionTestKit(testKit)

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    // don't use futureValue (patience) here because it can take a while to start the test container
    Await.result(ContainerSessionProvider.started, 30.seconds)
    // reason for setSchemaMetadataEnabled is that it speed up tests
    session.underlying().map(_.setSchemaMetadataEnabled(false)).futureValue
    Await.result(offsetStore.createKeyspaceAndTable(), 15.seconds)
    Await.result(repository.createKeyspaceAndTable(), 15.seconds)
    session.underlying().map(_.setSchemaMetadataEnabled(null)).futureValue
  }

  override protected def afterAll(): Unit = {
    // reason for setSchemaMetadataEnabled is that it speed up tests
    Try(session.underlying().map(_.setSchemaMetadataEnabled(false)).futureValue)
    Try(session.executeDDL(s"DROP keyspace ${offsetStore.keyspace}").futureValue)
    Try(session.executeDDL(s"DROP keyspace ${repository.keyspace}").futureValue)
    Try(session.underlying().map(_.setSchemaMetadataEnabled(null)).futureValue)
    super.afterAll()
  }

  private def genRandomProjectionId() =
    ProjectionId(UUID.randomUUID().toString, UUID.randomUUID().toString)

  @tailrec private def eventuallyExpectError(sinkProbe: TestSubscriber.Probe[_]): Throwable = {
    sinkProbe.expectNextOrError() match {
      case Right(_)  => eventuallyExpectError(sinkProbe)
      case Left(exc) => exc
    }
  }

  "A Cassandra at-least-once projection" must {

    "persist projection and offset" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      val projection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](
            projectionId,
            sourceProvider(entityId),
            saveOffsetAfterEnvelopes = 1,
            saveOffsetAfterDuration = Duration.Zero) { envelope =>
            repository.concatToText(envelope.id, envelope.message)
          }

      projectionTestKit.run(projection) {
        withClue("check - all values were concatenated") {
          val concatStr = repository.findById(entityId).futureValue.get
          concatStr.text shouldBe "abc|def|ghi|jkl|mno|pqr"
        }
      }
      withClue("check - all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.get
        offset shouldBe 6L
      }
    }

    "restart from previous offset - handler throwing an exception, save after 1" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val streamFailureMsg = "fail on fourth envelope"
      val failingProjection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](
            projectionId,
            sourceProvider(entityId),
            saveOffsetAfterEnvelopes = 1,
            saveOffsetAfterDuration = Duration.Zero) { envelope =>
            if (envelope.offset == 4L) throw new RuntimeException(streamFailureMsg)
            repository.concatToText(envelope.id, envelope.message)
          }

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      withClue("check: projection failed with stream failure") {
        val sinkProbe = projectionTestKit.runWithTestSink(failingProjection)
        sinkProbe.request(1000)
        eventuallyExpectError(sinkProbe).getMessage shouldBe streamFailureMsg
      }
      withClue("check: projection is consumed up to third") {
        val concatStr = repository.findById(entityId).futureValue.get
        concatStr.text shouldBe "abc|def|ghi"
      }
      withClue("check: last seen offset is 3L") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.get
        offset shouldBe 3L
      }

      // re-run projection without failing function
      val projection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](
            projectionId,
            sourceProvider(entityId),
            saveOffsetAfterEnvelopes = 1,
            saveOffsetAfterDuration = Duration.Zero) { envelope =>
            repository.concatToText(envelope.id, envelope.message)
          }

      projectionTestKit.run(projection) {
        withClue("checking: all values were concatenated") {
          val concatStr = repository.findById(entityId).futureValue.get
          concatStr.text shouldBe "abc|def|ghi|jkl|mno|pqr"
        }
      }

      withClue("check: all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.get
        offset shouldBe 6L
      }
    }

    "restart from previous offset - handler throwing an exception, save after 2" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val streamFailureMsg = "fail on fourth envelope"
      val failingProjection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](
            projectionId,
            sourceProvider(entityId),
            saveOffsetAfterEnvelopes = 2,
            saveOffsetAfterDuration = 1.minute) { envelope =>
            if (envelope.offset == 4L) throw new RuntimeException(streamFailureMsg)
            repository.concatToText(envelope.id, envelope.message)
          }

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      withClue("check: projection failed with stream failure") {
        val sinkProbe = projectionTestKit.runWithTestSink(failingProjection)
        sinkProbe.request(1000)
        eventuallyExpectError(sinkProbe).getMessage shouldBe streamFailureMsg
      }
      withClue("check: projection is consumed up to third") {
        val concatStr = repository.findById(entityId).futureValue.get
        concatStr.text shouldBe "abc|def|ghi"
      }
      withClue("check: last seen offset is 2L") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.get
        offset shouldBe 2L
      }

      // re-run projection without failing function
      val projection =
        CassandraProjection
          .atLeastOnce[Long, Envelope](
            projectionId,
            sourceProvider(entityId),
            saveOffsetAfterEnvelopes = 2,
            saveOffsetAfterDuration = 1.minute) { envelope =>
            repository.concatToText(envelope.id, envelope.message)
          }

      projectionTestKit.run(projection) {
        withClue("checking: all values were concatenated") {
          val concatStr = repository.findById(entityId).futureValue.get
          // note that 3rd is duplicated
          concatStr.text shouldBe "abc|def|ghi|ghi|jkl|mno|pqr"
        }
      }

      withClue("check: all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.get
        offset shouldBe 6L
      }
    }

    "save offset after number of envelopes" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      import akka.actor.typed.scaladsl.adapter._
      val sourceProbe = new AtomicReference[TestPublisher.Probe[Envelope]]()
      val source = TestSource.probe[Envelope](system.toClassic).mapMaterializedValue { probe =>
        sourceProbe.set(probe)
        NotUsed
      }

      val projection =
        CassandraProjection.atLeastOnce[Long, Envelope](
          projectionId,
          TestSourceProvider(source),
          saveOffsetAfterEnvelopes = 10,
          saveOffsetAfterDuration = 1.minute) { envelope =>
          repository.concatToText(envelope.id, envelope.message)
        }

      val sinkProbe = projectionTestKit.runWithTestSink(projection)
      eventually {
        sourceProbe.get should not be null
      }
      sinkProbe.request(1000)

      (1 to 15).foreach { n =>
        sourceProbe.get.sendNext(Envelope(entityId, n, s"elem-$n"))
      }
      eventually {
        repository.findById(entityId).futureValue.get.text should include("elem-15")
      }
      offsetStore.readOffset[Long](projectionId).futureValue.get shouldBe 10L

      (16 to 22).foreach { n =>
        sourceProbe.get.sendNext(Envelope(entityId, n, s"elem-$n"))
      }
      eventually {
        repository.findById(entityId).futureValue.get.text should include("elem-22")
      }
      offsetStore.readOffset[Long](projectionId).futureValue.get shouldBe 20L

      sinkProbe.cancel()
    }

    "save offset after idle duration" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      import akka.actor.typed.scaladsl.adapter._
      val sourceProbe = new AtomicReference[TestPublisher.Probe[Envelope]]()
      val source = TestSource.probe[Envelope](system.toClassic).mapMaterializedValue { probe =>
        sourceProbe.set(probe)
        NotUsed
      }

      val projection =
        CassandraProjection.atLeastOnce[Long, Envelope](
          projectionId,
          TestSourceProvider(source),
          saveOffsetAfterEnvelopes = 10,
          saveOffsetAfterDuration = 2.seconds) { envelope =>
          repository.concatToText(envelope.id, envelope.message)
        }

      val sinkProbe = projectionTestKit.runWithTestSink(projection)
      eventually {
        sourceProbe.get should not be null
      }
      sinkProbe.request(1000)

      (1 to 15).foreach { n =>
        sourceProbe.get.sendNext(Envelope(entityId, n, s"elem-$n"))
      }
      eventually {
        repository.findById(entityId).futureValue.get.text should include("elem-15")
      }
      offsetStore.readOffset[Long](projectionId).futureValue.get shouldBe 10L

      (16 to 17).foreach { n =>
        sourceProbe.get.sendNext(Envelope(entityId, n, s"elem-$n"))
      }
      eventually {
        offsetStore.readOffset[Long](projectionId).futureValue.get shouldBe 17L
      }
      repository.findById(entityId).futureValue.get.text should include("elem-17")

      sinkProbe.cancel()
    }
  }

  "A Cassandra at-most-once projection" must {

    "persist projection and offset" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      val projection =
        CassandraProjection.atMostOnce[Long, Envelope](projectionId, sourceProvider(entityId)) { envelope =>
          repository.concatToText(envelope.id, envelope.message)
        }

      projectionTestKit.run(projection) {
        withClue("check - all values were concatenated") {
          val concatStr = repository.findById(entityId).futureValue.get
          concatStr.text shouldBe "abc|def|ghi|jkl|mno|pqr"
        }
      }
      withClue("check - all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.get
        offset shouldBe 6L
      }
    }

    "restart from next offset - handler throwing an exception" in {
      val entityId = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val streamFailureMsg = "fail on fourth envelope"
      val failingProjection =
        CassandraProjection.atMostOnce[Long, Envelope](projectionId, sourceProvider(entityId)) { envelope =>
          if (envelope.offset == 4L) throw new RuntimeException(streamFailureMsg)
          repository.concatToText(envelope.id, envelope.message)
        }

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      withClue("check: projection failed with stream failure") {
        val sinkProbe = projectionTestKit.runWithTestSink(failingProjection)
        sinkProbe.request(1000)
        eventuallyExpectError(sinkProbe).getMessage shouldBe streamFailureMsg
      }
      withClue("check: projection is consumed up to third") {
        val concatStr = repository.findById(entityId).futureValue.get
        concatStr.text shouldBe "abc|def|ghi"
      }
      withClue("check: last seen offset is 4L") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.get
        offset shouldBe 4L // offset saved before handler for at-most-once
      }

      // re-run projection without failing function
      val projection =
        CassandraProjection.atMostOnce[Long, Envelope](projectionId, sourceProvider(entityId)) { envelope =>
          repository.concatToText(envelope.id, envelope.message)
        }

      projectionTestKit.run(projection) {
        withClue("checking: all values were concatenated") {
          val concatStr = repository.findById(entityId).futureValue.get
          // failed: jkl not included
          concatStr.text shouldBe "abc|def|ghi|mno|pqr"
        }
      }

      withClue("check: all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.get
        offset shouldBe 6L
      }
    }
  }
}
