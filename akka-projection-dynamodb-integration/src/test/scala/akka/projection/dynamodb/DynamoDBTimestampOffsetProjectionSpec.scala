/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.dynamodb

import java.time.Instant
import java.time.{ Duration => JDuration }
import java.util.Optional
import java.util.UUID
import java.util.concurrent.CompletionException
import java.util.concurrent.CompletionStage
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Supplier
import java.util.{ HashMap => JHashMap }

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._
import scala.jdk.FutureConverters._
import scala.jdk.OptionConverters._
import scala.util.Failure
import scala.util.Success

import akka.Done
import akka.NotUsed
import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.persistence.dynamodb.internal.EnvelopeOrigin
import akka.persistence.query.Offset
import akka.persistence.query.TimestampOffset
import akka.persistence.query.TimestampOffsetBySlice
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.EventTimestampQuery
import akka.persistence.query.typed.scaladsl.LoadEventQuery
import akka.persistence.typed.PersistenceId
import akka.projection.BySlicesSourceProvider
import akka.projection.HandlerRecoveryStrategy
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionContext
import akka.projection.ProjectionId
import akka.projection.TestStatusObserver
import akka.projection.TestStatusObserver.Err
import akka.projection.TestStatusObserver.OffsetProgress
import akka.projection.dynamodb.internal.DynamoDBOffsetStore
import akka.projection.dynamodb.internal.DynamoDBOffsetStore.Pid
import akka.projection.dynamodb.internal.DynamoDBOffsetStore.SeqNr
import akka.projection.dynamodb.scaladsl.DynamoDBProjection
import akka.projection.dynamodb.scaladsl.DynamoDBTransactHandler
import akka.projection.eventsourced.scaladsl.EventSourcedProvider.LoadEventsByPersistenceIdSourceProvider
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.SourceProvider
import akka.projection.testkit.scaladsl.ProjectionTestKit
import akka.projection.testkit.scaladsl.TestSourceProvider
import akka.stream.scaladsl.FlowWithContext
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSource
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import software.amazon.awssdk.services.dynamodb.model.KeyType
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput
import software.amazon.awssdk.services.dynamodb.model.Put
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem

object DynamoDBTimestampOffsetProjectionSpec {

  final case class Envelope(id: String, seqNr: Long, message: String)

  /**
   * This variant of TestStatusObserver is useful when the incoming envelope is the original akka projection
   * EventBySliceEnvelope, but we want to assert on [[Envelope]]. The original [[EventEnvelope]] has too many params
   * that are not so interesting for the test including the offset timestamp that would make the it harder to test.
   */
  class DynamoDBTestStatusObserver(
      statusProbe: ActorRef[TestStatusObserver.Status],
      progressProbe: ActorRef[TestStatusObserver.OffsetProgress[Envelope]])
      extends TestStatusObserver[EventEnvelope[String]](statusProbe.ref) {
    override def offsetProgress(projectionId: ProjectionId, envelope: EventEnvelope[String]): Unit =
      progressProbe ! OffsetProgress(
        Envelope(envelope.persistenceId, envelope.sequenceNr, envelope.eventOption.getOrElse("None")))

    override def error(
        projectionId: ProjectionId,
        envelope: EventEnvelope[String],
        cause: Throwable,
        recoveryStrategy: HandlerRecoveryStrategy): Unit =
      statusProbe ! Err(
        Envelope(envelope.persistenceId, envelope.sequenceNr, envelope.eventOption.getOrElse("None")),
        cause)
  }

  class TestTimestampSourceProvider(
      envelopes: immutable.IndexedSeq[EventEnvelope[String]],
      testSourceProvider: TestSourceProvider[Offset, EventEnvelope[String]],
      override val maxSlice: Int,
      enableCurrentEventsByPersistenceId: Boolean)
      extends SourceProvider[Offset, EventEnvelope[String]]
      with BySlicesSourceProvider
      with EventTimestampQuery
      with LoadEventQuery
      with LoadEventsByPersistenceIdSourceProvider[String] {

    override def source(offset: () => Future[Option[Offset]]): Future[Source[EventEnvelope[String], NotUsed]] =
      testSourceProvider.source(offset)

    override def extractOffset(envelope: EventEnvelope[String]): Offset =
      testSourceProvider.extractOffset(envelope)

    override def extractCreationTime(envelope: EventEnvelope[String]): Long =
      testSourceProvider.extractCreationTime(envelope)

    override def minSlice: Int = 0

    override def timestampOf(persistenceId: String, sequenceNr: Long): Future[Option[Instant]] = {
      Future.successful(envelopes.collectFirst {
        case env
            if env.persistenceId == persistenceId && env.sequenceNr == sequenceNr && env.offset
              .isInstanceOf[TimestampOffset] =>
          env.offset.asInstanceOf[TimestampOffset].timestamp
      })
    }

    override def loadEnvelope[Event](persistenceId: String, sequenceNr: Long): Future[EventEnvelope[Event]] = {
      envelopes.collectFirst {
        case env if env.persistenceId == persistenceId && env.sequenceNr == sequenceNr =>
          env.asInstanceOf[EventEnvelope[Event]]
      } match {
        case Some(env) => Future.successful(env)
        case None =>
          Future.failed(
            new NoSuchElementException(
              s"Event with persistenceId [$persistenceId] and sequenceNr [$sequenceNr] not found."))
      }
    }

    override private[akka] def currentEventsByPersistenceId(
        persistenceId: String,
        fromSequenceNr: Long,
        toSequenceNr: Long): Option[Source[EventEnvelope[String], NotUsed]] = {
      if (enableCurrentEventsByPersistenceId) {
        // also handle sequence number resets, which will have duplicate events, by first filtering to the last occurrence
        val skipEnvelopes = envelopes.lastIndexWhere { env =>
          env.persistenceId == persistenceId && env.sequenceNr == fromSequenceNr
        }
        Some(Source(envelopes.drop(skipEnvelopes).filter { env =>
          env.persistenceId == persistenceId && env.sequenceNr >= fromSequenceNr && env.sequenceNr <= toSequenceNr
        }))
      } else
        None
    }
  }

  class JavaTestTimestampSourceProvider(
      envelopes: immutable.IndexedSeq[EventEnvelope[String]],
      testSourceProvider: akka.projection.testkit.javadsl.TestSourceProvider[Offset, EventEnvelope[String]],
      override val maxSlice: Int,
      enableCurrentEventsByPersistenceId: Boolean)
      extends akka.projection.javadsl.SourceProvider[Offset, EventEnvelope[String]]
      with BySlicesSourceProvider
      with akka.persistence.query.typed.javadsl.EventTimestampQuery
      with akka.persistence.query.typed.javadsl.LoadEventQuery
      with LoadEventsByPersistenceIdSourceProvider[String] {

    override def source(offset: Supplier[CompletionStage[Optional[Offset]]])
        : CompletionStage[akka.stream.javadsl.Source[EventEnvelope[String], NotUsed]] =
      testSourceProvider.source(offset)

    override def extractOffset(envelope: EventEnvelope[String]): Offset =
      testSourceProvider.extractOffset(envelope)

    override def extractCreationTime(envelope: EventEnvelope[String]): Long =
      testSourceProvider.extractCreationTime(envelope)

    override def minSlice: Int = 0

    override def timestampOf(persistenceId: String, sequenceNr: Long): CompletionStage[Optional[Instant]] = {
      Future
        .successful(envelopes.collectFirst {
          case env
              if env.persistenceId == persistenceId && env.sequenceNr == sequenceNr && env.offset
                .isInstanceOf[TimestampOffset] =>
            env.offset.asInstanceOf[TimestampOffset].timestamp
        }.toJava)
        .asJava
    }

    override def loadEnvelope[Event](persistenceId: String, sequenceNr: Long): CompletionStage[EventEnvelope[Event]] = {
      envelopes.collectFirst {
        case env if env.persistenceId == persistenceId && env.sequenceNr == sequenceNr =>
          env.asInstanceOf[EventEnvelope[Event]]
      } match {
        case Some(env) => Future.successful(env).asJava
        case None =>
          Future
            .failed(
              new NoSuchElementException(
                s"Event with persistenceId [$persistenceId] and sequenceNr [$sequenceNr] not found."))
            .asJava
      }
    }

    override private[akka] def currentEventsByPersistenceId(
        persistenceId: String,
        fromSequenceNr: Long,
        toSequenceNr: Long): Option[Source[EventEnvelope[String], NotUsed]] = {
      if (enableCurrentEventsByPersistenceId)
        Some(Source(envelopes.filter { env =>
          env.persistenceId == persistenceId && env.sequenceNr >= fromSequenceNr && env.sequenceNr <= toSequenceNr
        }))
      else
        None
    }
  }

  // test model is as simple as a text that gets other string concatenated to it
  case class ConcatStr(id: String, text: String) {
    def concat(newMsg: String): ConcatStr = {
      if (text == "")
        copy(id, newMsg)
      else
        copy(text = text + "|" + newMsg)
    }
  }

  final case class TestRepository()(implicit ec: ExecutionContext) {
    private val store = new ConcurrentHashMap[String, String]()

    private val logger = LoggerFactory.getLogger(this.getClass)

    def concatToText(id: String, payload: String): Future[Done] = {
      val savedStrOpt = findById(id)

      savedStrOpt.flatMap { strOpt =>
        val newConcatStr = strOpt
          .map {
            _.concat(payload)
          }
          .getOrElse(ConcatStr(id, payload))

        upsert(newConcatStr)
      }
    }

    def update(id: String, payload: String): Future[Done] = {
      upsert(ConcatStr(id, payload))
    }

    def updateWithNullValue(id: String): Future[Done] = {
      upsert(ConcatStr(id, null))
    }

    private def upsert(concatStr: ConcatStr): Future[Done] = {
      logger.debug("TestRepository.upsert: [{}]", concatStr)
      store.put(concatStr.id, concatStr.text)
      Future.successful(Done)
    }

    def findById(id: String): Future[Option[ConcatStr]] = {
      logger.debug("TestRepository.findById: [{}]", id)
      Future.successful(Option(store.get(id)).map(text => ConcatStr(id, text)))
    }

  }

  object TestTable {
    val name = "projection_spec"

    object Attributes {
      val Id = "id"
      val Payload = "payload"
    }

    def create(client: DynamoDbAsyncClient, system: ActorSystem[_]): Future[Done] = {
      import system.executionContext

      client.describeTable(DescribeTableRequest.builder().tableName(name).build()).asScala.transformWith {
        case Success(_) => Future.successful(Done) // already exists
        case Failure(_) =>
          val request = CreateTableRequest
            .builder()
            .tableName(name)
            .keySchema(KeySchemaElement.builder().attributeName(Attributes.Id).keyType(KeyType.HASH).build())
            .attributeDefinitions(
              AttributeDefinition.builder().attributeName(Attributes.Id).attributeType(ScalarAttributeType.S).build())
            .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(5L).writeCapacityUnits(5L).build())
            .build()
          client
            .createTable(request)
            .asScala
            .map(_ => Done)
            .recoverWith {
              case c: CompletionException =>
                Future.failed(c.getCause)
            }(ExecutionContext.parasitic)
      }
    }

    def findById(id: String)(client: DynamoDbAsyncClient, system: ActorSystem[_]): Future[Option[ConcatStr]] = {
      import system.executionContext

      client
        .getItem(
          GetItemRequest
            .builder()
            .tableName(name)
            .consistentRead(true)
            .key(Map(Attributes.Id -> AttributeValue.fromS(id)).asJava)
            .build())
        .asScala
        .map { response =>
          if (response.hasItem && response.item.containsKey(Attributes.Payload))
            Some(ConcatStr(id, response.item.get(Attributes.Payload).s()))
          else None
        }
        .recoverWith {
          case c: CompletionException =>
            Future.failed(c.getCause)
        }(ExecutionContext.parasitic)
    }
  }

}

class DynamoDBTimestampOffsetProjectionSpec
    extends ScalaTestWithActorTestKit(TestConfig.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {
  import DynamoDBTimestampOffsetProjectionSpec._

  override def typedSystem: ActorSystem[_] = system
  private implicit val ec: ExecutionContext = system.executionContext

  private val logger = LoggerFactory.getLogger(getClass)
  private def createOffsetStore(
      projectionId: ProjectionId,
      sourceProvider: TestTimestampSourceProvider): DynamoDBOffsetStore =
    new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)
  private val projectionTestKit = ProjectionTestKit(system)

  private val repository = new TestRepository()

  override protected def beforeAll(): Unit = {
    if (localDynamoDB)
      Await.result(TestTable.create(client, system), 10.seconds)
    super.beforeAll()
  }

  def createSourceProvider(
      envelopes: immutable.IndexedSeq[EventEnvelope[String]],
      enableCurrentEventsByPersistenceId: Boolean = false,
      complete: Boolean = true): TestTimestampSourceProvider = {
    createSourceProviderWithMoreEnvelopes(envelopes, envelopes, enableCurrentEventsByPersistenceId, complete)
  }

  // envelopes are emitted by the "query" source, but allEnvelopes can be loaded
  def createSourceProviderWithMoreEnvelopes(
      envelopes: immutable.IndexedSeq[EventEnvelope[String]],
      allEnvelopes: immutable.IndexedSeq[EventEnvelope[String]],
      enableCurrentEventsByPersistenceId: Boolean,
      complete: Boolean = true): TestTimestampSourceProvider = {
    createTestSourceProvider(Source(envelopes), allEnvelopes, enableCurrentEventsByPersistenceId, complete)
  }

  def createDynamicSourceProvider(
      allEnvelopes: immutable.IndexedSeq[EventEnvelope[String]],
      enableCurrentEventsByPersistenceId: Boolean = false,
      complete: Boolean = true): (TestTimestampSourceProvider, Future[TestPublisher.Probe[EventEnvelope[String]]]) = {
    val sourceProbe = Promise[TestPublisher.Probe[EventEnvelope[String]]]()
    val source = TestSource[EventEnvelope[String]]().mapMaterializedValue { probe =>
      sourceProbe.success(probe)
      NotUsed
    }
    val sourceProvider = createTestSourceProvider(source, allEnvelopes, enableCurrentEventsByPersistenceId, complete)
    (sourceProvider, sourceProbe.future)
  }

  def createTestSourceProvider(
      envelopesSource: Source[EventEnvelope[String], NotUsed],
      allEnvelopes: immutable.IndexedSeq[EventEnvelope[String]],
      enableCurrentEventsByPersistenceId: Boolean,
      complete: Boolean = true): TestTimestampSourceProvider = {
    val sp =
      TestSourceProvider[Offset, EventEnvelope[String]](envelopesSource, _.offset)
        .withStartSourceFrom {
          case (lastProcessedOffsetBySlice: TimestampOffsetBySlice, offset: TimestampOffset) =>
            // FIXME: should have the envelope slice to handle this properly
            val lastProcessedOffset = lastProcessedOffsetBySlice.offsets.head._2
            offset.timestamp.isBefore(lastProcessedOffset.timestamp) ||
            (offset.timestamp == lastProcessedOffset.timestamp && offset.seen == lastProcessedOffset.seen)
          case _ => false
        }
        .withAllowCompletion(complete)

    new TestTimestampSourceProvider(
      allEnvelopes,
      sp,
      persistenceExt.numberOfSlices - 1,
      enableCurrentEventsByPersistenceId)
  }

  // envelopes are emitted by the "query" source, but allEnvelopes can be loaded
  def createJavaSourceProviderWithMoreEnvelopes(
      envelopes: immutable.IndexedSeq[EventEnvelope[String]],
      allEnvelopes: immutable.IndexedSeq[EventEnvelope[String]],
      enableCurrentEventsByPersistenceId: Boolean,
      complete: Boolean = true): JavaTestTimestampSourceProvider = {
    val sp =
      akka.projection.testkit.javadsl.TestSourceProvider
        .create[Offset, EventEnvelope[String]](akka.stream.javadsl.Source.from(envelopes.asJava), _.offset)
        .withStartSourceFrom {
          case (lastProcessedOffsetBySlice: TimestampOffsetBySlice, offset: TimestampOffset) =>
            // FIXME: should have the envelope slice to handle this properly
            val lastProcessedOffset = lastProcessedOffsetBySlice.offsets.head._2
            offset.timestamp.isBefore(lastProcessedOffset.timestamp) ||
            (offset.timestamp == lastProcessedOffset.timestamp && offset.seen == lastProcessedOffset.seen)
          case _ => false
        }
        .withAllowCompletion(complete)

    new JavaTestTimestampSourceProvider(
      allEnvelopes,
      sp,
      persistenceExt.numberOfSlices - 1,
      enableCurrentEventsByPersistenceId)
  }

  def createBacktrackingSourceProvider(
      envelopes: immutable.IndexedSeq[EventEnvelope[String]],
      complete: Boolean = true): TestTimestampSourceProvider = {
    val sp =
      TestSourceProvider[Offset, EventEnvelope[String]](Source(envelopes), _.offset)
        .withStartSourceFrom { (_, _) => false } // include all
        .withAllowCompletion(complete)
    new TestTimestampSourceProvider(
      envelopes,
      sp,
      persistenceExt.numberOfSlices - 1,
      enableCurrentEventsByPersistenceId = false)
  }

  private def latestOffsetShouldBe(expected: Any)(implicit offsetStore: DynamoDBOffsetStore) = {
    val expectedTimestampOffset = expected.asInstanceOf[TimestampOffset]
    offsetStore.readOffset().futureValue
    offsetStore.getState().latestOffset shouldBe expectedTimestampOffset
  }

  private def offsetShouldBeEmpty()(implicit offsetStore: DynamoDBOffsetStore) = {
    offsetStore.readOffset[TimestampOffsetBySlice]().futureValue shouldBe empty
  }

  private def projectedValueShouldBe(expected: String)(implicit entityId: String) = {
    val opt = repository.findById(entityId).futureValue.map(_.text)
    opt shouldBe Some(expected)
  }

  private def projectedTestValueShouldBe(expected: String)(implicit entityId: String) = {
    val opt = TestTable.findById(entityId)(client, system).futureValue.map(_.text)
    opt shouldBe Some(expected)
  }

  // TODO: extract this to some utility
  @tailrec private def eventuallyExpectError(sinkProbe: TestSubscriber.Probe[_]): Throwable = {
    sinkProbe.expectNextOrError() match {
      case Right(_)  => eventuallyExpectError(sinkProbe)
      case Left(exc) => exc
    }
  }

  private val concatHandlerFail4Msg = "fail on fourth envelope"

  class ConcatHandler(repository: TestRepository, failPredicate: EventEnvelope[String] => Boolean = _ => false)
      extends Handler[EventEnvelope[String]] {

    private val logger = LoggerFactory.getLogger(getClass)
    private val _attempts = new AtomicInteger()
    def attempts: Int = _attempts.get

    override def process(envelope: EventEnvelope[String]): Future[Done] = {
      if (failPredicate(envelope)) {
        _attempts.incrementAndGet()
        throw TestException(concatHandlerFail4Msg + s" after $attempts attempts")
      } else {
        logger.debug(s"handling {}", envelope)
        repository.concatToText(envelope.persistenceId, envelope.event)
      }
    }

  }

  class TransactConcatHandler(failPredicate: EventEnvelope[String] => Boolean = _ => false)
      extends DynamoDBTransactHandler[EventEnvelope[String]] {

    private val logger = LoggerFactory.getLogger(getClass)
    private val _attempts = new AtomicInteger()
    def attempts: Int = _attempts.get

    override def process(envelope: EventEnvelope[String]): Future[Iterable[TransactWriteItem]] = {
      if (failPredicate(envelope)) {
        _attempts.incrementAndGet()
        throw TestException(concatHandlerFail4Msg + s" after $attempts attempts")
      } else {
        logger.debug(s"handling {}", envelope)
        TestTable.findById(envelope.persistenceId)(client, system).map { current =>
          val newPayload = current.fold(envelope.event)(_.concat(envelope.event).text)
          val attributes = new JHashMap[String, AttributeValue]
          attributes.put(TestTable.Attributes.Id, AttributeValue.fromS(envelope.persistenceId))
          attributes.put(TestTable.Attributes.Payload, AttributeValue.fromS(newPayload))
          Seq(TransactWriteItem.builder().put(Put.builder().tableName(TestTable.name).item(attributes).build()).build())
        }
      }
    }
  }

  class TransactGroupedConcatHandler(
      probe: ActorRef[String],
      failPredicate: EventEnvelope[String] => Boolean = _ => false)
      extends DynamoDBTransactHandler[Seq[EventEnvelope[String]]] {

    private val _attempts = new AtomicInteger()
    def attempts: Int = _attempts.get

    override def process(envelopes: Seq[EventEnvelope[String]]): Future[Iterable[TransactWriteItem]] = {
      probe ! "called"

      // can only have one TransactWriteItem per key
      val envelopesByPid = envelopes.groupBy(_.persistenceId)

      Future.sequence(envelopesByPid.map {
        case (pid, pidEnvelopes) =>
          TestTable.findById(pid)(client, system).map {
            current =>
              val newConcatStr = pidEnvelopes.foldLeft(current.getOrElse(ConcatStr(pid, ""))) { (acc, env) =>
                if (failPredicate(env)) {
                  _attempts.incrementAndGet()
                  throw TestException(concatHandlerFail4Msg + s" after $attempts attempts")
                }
                acc.concat(env.event)
              }

              val attributes = new JHashMap[String, AttributeValue]
              attributes.put(TestTable.Attributes.Id, AttributeValue.fromS(pid))
              attributes.put(TestTable.Attributes.Payload, AttributeValue.fromS(newConcatStr.text))
              TransactWriteItem.builder().put(Put.builder().tableName(TestTable.name).item(attributes).build()).build()
          }
      })
    }
  }

  private val clock = TestClock.nowMicros()
  def tick(): TestClock = {
    clock.tick(JDuration.ofMillis(1))
    clock
  }

  def createEnvelope(pid: Pid, seqNr: SeqNr, timestamp: Instant, event: String): EventEnvelope[String] = {
    val entityType = PersistenceId.extractEntityType(pid)
    val slice = persistenceExt.sliceForPersistenceId(pid)
    EventEnvelope(
      TimestampOffset(timestamp, timestamp.plusMillis(1000), Map(pid -> seqNr)),
      pid,
      seqNr,
      event,
      timestamp.toEpochMilli,
      entityType,
      slice)
  }

  def backtrackingEnvelope(env: EventEnvelope[String]): EventEnvelope[String] =
    new EventEnvelope[String](
      env.offset,
      env.persistenceId,
      env.sequenceNr,
      eventOption = None,
      env.timestamp,
      env.internalEventMetadata,
      env.entityType,
      env.slice,
      env.filtered,
      source = EnvelopeOrigin.SourceBacktracking)

  def createEnvelopes(pid: Pid, numberOfEvents: Int): immutable.IndexedSeq[EventEnvelope[String]] = {
    (1 to numberOfEvents).map { n =>
      createEnvelope(pid, n, tick().instant(), s"e$n")
    }
  }

  def createEnvelopesWithDuplicates(pid1: Pid, pid2: Pid): Vector[EventEnvelope[String]] = {
    val startTime = TestClock.nowMicros().instant()

    Vector(
      createEnvelope(pid1, 1, startTime, s"e1-1"),
      createEnvelope(pid1, 2, startTime.plusMillis(1), s"e1-2"),
      createEnvelope(pid2, 1, startTime.plusMillis(2), s"e2-1"),
      createEnvelope(pid1, 3, startTime.plusMillis(4), s"e1-3"),
      // pid1-3 is emitted before pid2-2 even though pid2-2 timestamp is earlier,
      // from backtracking query previous events are emitted again, including the missing pid2-2
      createEnvelope(pid1, 1, startTime, s"e1-1"),
      createEnvelope(pid1, 2, startTime.plusMillis(1), s"e1-2"),
      createEnvelope(pid2, 1, startTime.plusMillis(2), s"e2-1"),
      // now it pid2-2 is included
      createEnvelope(pid2, 2, startTime.plusMillis(3), s"e2-2"),
      createEnvelope(pid1, 3, startTime.plusMillis(4), s"e1-3"),
      // and then some normal again
      createEnvelope(pid1, 4, startTime.plusMillis(5), s"e1-4"),
      createEnvelope(pid2, 3, startTime.plusMillis(6), s"e2-3"))
  }

  def createEnvelopesUnknownSequenceNumbers(startTime: Instant, pid1: Pid, pid2: Pid): Vector[EventEnvelope[String]] = {
    Vector(
      createEnvelope(pid1, 1, startTime, s"e1-1"),
      createEnvelope(pid1, 2, startTime.plusMillis(1), s"e1-2"),
      createEnvelope(pid2, 1, startTime.plusMillis(2), s"e2-1"),
      // pid2 seqNr 2 missing, will reject 3
      createEnvelope(pid2, 3, startTime.plusMillis(4), s"e2-3"),
      createEnvelope(pid1, 3, startTime.plusMillis(5), s"e1-3"),
      // pid1 seqNr 4 missing, will reject 5
      createEnvelope(pid1, 5, startTime.plusMillis(7), s"e1-5"))
  }

  def createEnvelopesBacktrackingUnknownSequenceNumbers(
      startTime: Instant,
      pid1: Pid,
      pid2: Pid): Vector[EventEnvelope[String]] = {
    Vector(
      // may also contain some duplicates
      createEnvelope(pid1, 2, startTime.plusMillis(1), s"e1-2"),
      createEnvelope(pid2, 2, startTime.plusMillis(3), s"e2-2"),
      createEnvelope(pid2, 3, startTime.plusMillis(4), s"e2-3"),
      createEnvelope(pid1, 3, startTime.plusMillis(5), s"e1-3"),
      createEnvelope(pid1, 4, startTime.plusMillis(6), s"e1-4"),
      createEnvelope(pid1, 5, startTime.plusMillis(7), s"e1-5"),
      createEnvelope(pid2, 4, startTime.plusMillis(8), s"e2-4"),
      createEnvelope(pid1, 6, startTime.plusMillis(9), s"e1-6"))
  }

  def markAsFilteredEvent[A](env: EventEnvelope[A]): EventEnvelope[A] = {
    new EventEnvelope[A](
      env.offset,
      env.persistenceId,
      env.sequenceNr,
      env.eventOption,
      env.timestamp,
      env.internalEventMetadata,
      env.entityType,
      env.slice,
      filtered = true,
      env.source)
  }

  "A DynamoDB at-least-once projection with TimestampOffset" must {

    "persist projection and offset" in {
      implicit val pid = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes = createEnvelopes(pid, 6)
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val projection =
        DynamoDBProjection.atLeastOnce(
          projectionId,
          Some(settings),
          sourceProvider,
          handler = () => new ConcatHandler(repository))

      offsetShouldBeEmpty()
      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6")
      }
      eventually {
        latestOffsetShouldBe(envelopes.last.offset)
      }
    }

    "filter duplicates" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes = createEnvelopesWithDuplicates(pid1, pid2)
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val projection =
        DynamoDBProjection.atLeastOnce(
          projectionId,
          Some(settings),
          sourceProvider,
          handler = () => new ConcatHandler(repository))

      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1-1|e1-2|e1-3|e1-4")(pid1)
        projectedValueShouldBe("e2-1|e2-2|e2-3")(pid2)
      }
      eventually {
        latestOffsetShouldBe(envelopes.last.offset)
      }
    }

    "filter out unknown sequence numbers" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val startTime = TestClock.nowMicros().instant()
      val sourceProvider = new TestSourceProviderWithInput()
      implicit val offsetStore: DynamoDBOffsetStore =
        new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)

      val projectionRef = spawn(
        ProjectionBehavior(DynamoDBProjection
          .atLeastOnce(projectionId, Some(settings), sourceProvider, handler = () => new ConcatHandler(repository))))
      val input = sourceProvider.input.futureValue

      val envelopes1 = createEnvelopesUnknownSequenceNumbers(startTime, pid1, pid2)
      envelopes1.foreach(input ! _)

      eventually {
        projectedValueShouldBe("e1-1|e1-2|e1-3")(pid1)
        projectedValueShouldBe("e2-1")(pid2)
      }

      // simulate backtracking
      logger.debug("Starting backtracking")
      val envelopes2 = createEnvelopesBacktrackingUnknownSequenceNumbers(startTime, pid1, pid2)
      envelopes2.foreach(input ! _)

      eventually {
        projectedValueShouldBe("e1-1|e1-2|e1-3|e1-4|e1-5|e1-6")(pid1)
        projectedValueShouldBe("e2-1|e2-2|e2-3|e2-4")(pid2)
      }

      eventually {
        latestOffsetShouldBe(envelopes2.last.offset)
      }
      projectionRef ! ProjectionBehavior.Stop
    }

    "re-delivery inflight events after failure with retry recovery strategy" in {
      implicit val pid1 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes = createEnvelopes(pid1, 6)
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val failOnce = new AtomicBoolean(true)
      val failPredicate = (ev: EventEnvelope[String]) => {
        // fail on first call for event 4, let it pass afterwards
        ev.sequenceNr == 4 && failOnce.compareAndSet(true, false)
      }
      val bogusEventHandler = new ConcatHandler(repository, failPredicate)

      val projectionFailing =
        DynamoDBProjection
          .atLeastOnce(projectionId, Some(settings), sourceProvider, handler = () => bogusEventHandler)
          .withSaveOffset(afterEnvelopes = 5, afterDuration = 2.seconds)
          .withRecoveryStrategy(HandlerRecoveryStrategy.retryAndSkip(2, 10.millis))

      offsetShouldBeEmpty()
      projectionTestKit.run(projectionFailing) {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6")
      }

      bogusEventHandler.attempts shouldBe 1

      eventually {
        latestOffsetShouldBe(envelopes.last.offset)
      }
    }

    "be able to skip envelopes but still store offset" in {
      implicit val pid = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes = createEnvelopes(pid, 6).map { env =>
        if (env.event == "e3" || env.event == "e4" || env.event == "e6")
          markAsFilteredEvent(env)
        else
          env
      }
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val projection =
        DynamoDBProjection.atLeastOnce(
          projectionId,
          Some(settings),
          sourceProvider,
          handler = () => new ConcatHandler(repository))

      offsetShouldBeEmpty()
      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e5")
      }
      eventually {
        latestOffsetShouldBe(envelopes.last.offset)
      }
    }

    "handle async projection" in {
      implicit val pid = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes = createEnvelopes(pid, 6)
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val result = new StringBuffer()

      def handler(): Handler[EventEnvelope[String]] = new Handler[EventEnvelope[String]] {
        override def process(envelope: EventEnvelope[String]): Future[Done] = {
          Future
            .successful {
              result.append(envelope.event).append("|")
            }
            .map(_ => Done)
        }
      }

      val projection =
        DynamoDBProjection.atLeastOnce(projectionId, Some(settings), sourceProvider, handler = () => handler())

      projectionTestKit.run(projection) {
        result.toString shouldBe "e1|e2|e3|e4|e5|e6|"
      }
      eventually {
        latestOffsetShouldBe(envelopes.last.offset)
      }
    }

    "re-delivery inflight events after failure with retry recovery strategy for async projection" in {
      implicit val pid1 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes = createEnvelopes(pid1, 6)
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val failOnce = new AtomicBoolean(true)
      val failPredicate = (ev: EventEnvelope[String]) => {
        // fail on first call for event 4, let it pass afterwards
        ev.sequenceNr == 4 && failOnce.compareAndSet(true, false)
      }

      val result = new StringBuffer()
      def handler(): Handler[EventEnvelope[String]] = new Handler[EventEnvelope[String]] {
        override def process(envelope: EventEnvelope[String]): Future[Done] = {
          if (failPredicate(envelope)) {
            throw TestException(s"failed to process event '${envelope.sequenceNr}'")
          } else {
            Future
              .successful {
                result.append(envelope.event).append("|")
              }
              .map(_ => Done)
          }
        }
      }

      val projectionFailing =
        DynamoDBProjection
          .atLeastOnce(projectionId, Some(settings), sourceProvider, handler = () => handler())
          .withSaveOffset(afterEnvelopes = 5, afterDuration = 2.seconds)
          .withRecoveryStrategy(HandlerRecoveryStrategy.retryAndSkip(2, 10.millis))

      offsetShouldBeEmpty()
      projectionTestKit.run(projectionFailing) {
        result.toString shouldBe "e1|e2|e3|e4|e5|e6|"
      }

      eventually {
        latestOffsetShouldBe(envelopes.last.offset)
      }
    }

    "filter duplicates for async projection" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes = createEnvelopesWithDuplicates(pid1, pid2)
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val result1 = new StringBuffer()
      val result2 = new StringBuffer()

      def handler(): Handler[EventEnvelope[String]] = new Handler[EventEnvelope[String]] {
        override def process(envelope: EventEnvelope[String]): Future[Done] = {
          Future
            .successful {
              if (envelope.persistenceId == pid1)
                result1.append(envelope.event).append("|")
              else
                result2.append(envelope.event).append("|")
            }
            .map(_ => Done)
        }
      }

      val projection =
        DynamoDBProjection.atLeastOnce(projectionId, Some(settings), sourceProvider, handler = () => handler())

      projectionTestKit.run(projection) {
        result1.toString shouldBe "e1-1|e1-2|e1-3|e1-4|"
        result2.toString shouldBe "e2-1|e2-2|e2-3|"
      }
      eventually {
        latestOffsetShouldBe(envelopes.last.offset)
      }
    }

    "filter out unknown sequence numbers for async projection" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val startTime = TestClock.nowMicros().instant()
      val sourceProvider = new TestSourceProviderWithInput()
      implicit val offsetStore: DynamoDBOffsetStore =
        new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)

      val result1 = new StringBuffer()
      val result2 = new StringBuffer()

      def handler(): Handler[EventEnvelope[String]] = new Handler[EventEnvelope[String]] {
        override def process(envelope: EventEnvelope[String]): Future[Done] = {
          Future
            .successful {
              if (envelope.persistenceId == pid1)
                result1.append(envelope.event).append("|")
              else
                result2.append(envelope.event).append("|")
            }
            .map(_ => Done)
        }
      }

      val projectionRef = spawn(
        ProjectionBehavior(
          DynamoDBProjection.atLeastOnce(projectionId, Some(settings), sourceProvider, handler = () => handler())))
      val input = sourceProvider.input.futureValue

      val envelopes1 = createEnvelopesUnknownSequenceNumbers(startTime, pid1, pid2)
      envelopes1.foreach(input ! _)

      eventually {
        result1.toString shouldBe "e1-1|e1-2|e1-3|"
        result2.toString shouldBe "e2-1|"
      }

      // simulate backtracking
      logger.debug("Starting backtracking")
      val envelopes2 = createEnvelopesBacktrackingUnknownSequenceNumbers(startTime, pid1, pid2)
      envelopes2.foreach(input ! _)

      eventually {
        result1.toString shouldBe "e1-1|e1-2|e1-3|e1-4|e1-5|e1-6|"
        result2.toString shouldBe "e2-1|e2-2|e2-3|e2-4|"
      }

      eventually {
        latestOffsetShouldBe(envelopes2.last.offset)
      }
      projectionRef ! ProjectionBehavior.Stop
    }

    "be able to skip envelopes but still store offset for async projection" in {
      implicit val pid = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val envelopes = createEnvelopes(pid, 6).map { env =>
        if (env.event == "e3" || env.event == "e4" || env.event == "e6")
          markAsFilteredEvent(env)
        else
          env
      }

      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val projection =
        DynamoDBProjection.atLeastOnce(
          projectionId,
          Some(settings),
          sourceProvider,
          handler = () => new ConcatHandler(repository))

      offsetShouldBeEmpty()
      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e5")
      }
      eventually {
        latestOffsetShouldBe(envelopes.last.offset)
      }
    }

    "replay rejected sequence numbers" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val allEnvelopes = createEnvelopes(pid1, 6) ++ createEnvelopes(pid2, 3)
      val skipPid1SeqNrs = Set(3L, 4L, 5L)
      val envelopes = allEnvelopes.filterNot { env =>
        (env.persistenceId == pid1 && skipPid1SeqNrs(env.sequenceNr)) ||
        (env.persistenceId == pid2 && (env.sequenceNr == 1))
      }

      val sourceProvider =
        createSourceProviderWithMoreEnvelopes(envelopes, allEnvelopes, enableCurrentEventsByPersistenceId = true)

      implicit val offsetStore: DynamoDBOffsetStore =
        new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)

      val projectionRef = spawn(
        ProjectionBehavior(
          DynamoDBProjection
            .atLeastOnce(
              projectionId,
              Some(settings.withReplayOnRejectedSequenceNumbers(true)),
              sourceProvider,
              handler = () => new ConcatHandler(repository))))

      eventually {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6")(pid1)
        projectedValueShouldBe("e1|e2|e3")(pid2)
      }

      eventually {
        latestOffsetShouldBe(allEnvelopes.last.offset)
      }
      projectionRef ! ProjectionBehavior.Stop
    }

    "replay rejected sequence numbers due to clock skew on event write" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val start = tick().instant()

      def createEnvelopesFor(
          pid: Pid,
          fromSeqNr: Int,
          toSeqNr: Int,
          fromTimestamp: Instant): immutable.IndexedSeq[EventEnvelope[String]] = {
        (fromSeqNr to toSeqNr).map { n =>
          createEnvelope(pid, n, fromTimestamp.plusSeconds(n - fromSeqNr), s"e$n")
        }
      }

      val envelopes1 =
        createEnvelopesFor(pid1, 1, 2, start) ++
        createEnvelopesFor(pid1, 3, 4, start.plusSeconds(4)) ++ // gap
        createEnvelopesFor(pid1, 5, 9, start.plusSeconds(2)) // clock skew, back 2, and then overlapping

      val envelopes2 =
        createEnvelopesFor(pid2, 1, 3, start.plusSeconds(10)) ++
        createEnvelopesFor(pid2, 4, 6, start.plusSeconds(1)) ++ // clock skew, back 9
        createEnvelopesFor(pid2, 7, 9, start.plusSeconds(20)) // and gap

      val allEnvelopes = envelopes1 ++ envelopes2

      val envelopes = allEnvelopes.sortBy(_.timestamp)

      val sourceProvider =
        createSourceProviderWithMoreEnvelopes(envelopes, allEnvelopes, enableCurrentEventsByPersistenceId = true)

      implicit val offsetStore: DynamoDBOffsetStore =
        new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)

      val projectionRef = spawn(
        ProjectionBehavior(
          DynamoDBProjection
            .atLeastOnce(
              projectionId,
              Some(settings.withReplayOnRejectedSequenceNumbers(true)),
              sourceProvider,
              handler = () => new ConcatHandler(repository))))

      eventually {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6|e7|e8|e9")(pid1)
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6|e7|e8|e9")(pid2)
      }

      eventually {
        latestOffsetShouldBe(envelopes.last.offset)
      }

      projectionRef ! ProjectionBehavior.Stop
    }

    "accept events after detecting sequence number reset for at-least-once" in {
      val testSettings = settings.withAcceptSequenceNumberResetAfter(10.seconds)

      val pid1 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val start = tick().instant()

      def createEnvelopesFor(
          pid: Pid,
          fromSeqNr: Int,
          toSeqNr: Int,
          fromTimestamp: Instant): immutable.IndexedSeq[EventEnvelope[String]] = {
        (fromSeqNr to toSeqNr).map { n =>
          createEnvelope(pid, n, fromTimestamp.plusSeconds(n - fromSeqNr), s"e$n")
        }
      }

      val envelopes1 = createEnvelopesFor(pid1, 1, 5, start)
      val envelopes2 = createEnvelopesFor(pid1, 1, 3, start.plusSeconds(30))
      val allEnvelopes = envelopes1 ++ envelopes2

      val (sourceProvider, sourceProbe) = createDynamicSourceProvider(allEnvelopes)

      implicit val offsetStore: DynamoDBOffsetStore =
        new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)

      val projectionRef = spawn(ProjectionBehavior(DynamoDBProjection
        .atLeastOnce(projectionId, Some(testSettings), sourceProvider, handler = () => new ConcatHandler(repository))))

      val source = sourceProbe.futureValue

      envelopes1.foreach(source.sendNext)

      eventually {
        projectedValueShouldBe("e1|e2|e3|e4|e5")(pid1)
        latestOffsetShouldBe(envelopes1.last.offset)
      }

      envelopes2.foreach(source.sendNext)

      eventually {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e1|e2|e3")(pid1)
        latestOffsetShouldBe(envelopes2.last.offset)
      }

      projectionRef ! ProjectionBehavior.Stop
    }

    "replay rejected sequence numbers after detecting reset for at-least-once" in {
      val testSettings = settings.withAcceptSequenceNumberResetAfter(10.seconds)

      val pid1 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val start = tick().instant()

      def createEnvelopesFor(
          pid: Pid,
          fromSeqNr: Int,
          toSeqNr: Int,
          fromTimestamp: Instant): immutable.IndexedSeq[EventEnvelope[String]] = {
        (fromSeqNr to toSeqNr).map { n =>
          createEnvelope(pid, n, fromTimestamp.plusSeconds(n - fromSeqNr), s"e$n")
        }
      }

      val envelopes1 = createEnvelopesFor(pid1, 1, 5, start)
      val envelopes2 = createEnvelopesFor(pid1, 1, 5, start.plusSeconds(30))
      val allEnvelopes = envelopes1 ++ envelopes2

      val (sourceProvider, sourceProbe) =
        createDynamicSourceProvider(allEnvelopes, enableCurrentEventsByPersistenceId = true)

      implicit val offsetStore: DynamoDBOffsetStore =
        new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)

      val projectionRef = spawn(ProjectionBehavior(DynamoDBProjection
        .atLeastOnce(projectionId, Some(testSettings), sourceProvider, handler = () => new ConcatHandler(repository))))

      val source = sourceProbe.futureValue

      envelopes1.foreach(source.sendNext)

      eventually {
        projectedValueShouldBe("e1|e2|e3|e4|e5")(pid1)
        latestOffsetShouldBe(envelopes1.last.offset)
      }

      // start from seq nr 3 after reset
      envelopes2.drop(2).foreach(source.sendNext)

      eventually {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e1|e2|e3|e4|e5")(pid1)
        latestOffsetShouldBe(envelopes2.last.offset)
      }

      projectionRef ! ProjectionBehavior.Stop
    }
  }

  "A DynamoDB exactly-once projection with TimestampOffset" must {

    "persist projection and offset in the same write operation (transactional)" in {
      implicit val pid = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val envelopes = createEnvelopes(pid, 6)
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val projection =
        DynamoDBProjection.exactlyOnce(
          projectionId,
          Some(settings),
          sourceProvider,
          handler = () => new TransactConcatHandler)

      offsetShouldBeEmpty()
      projectionTestKit.run(projection) {
        projectedTestValueShouldBe("e1|e2|e3|e4|e5|e6")
      }
      latestOffsetShouldBe(envelopes.last.offset)
    }

    "skip failing events when using RecoveryStrategy.skip" in {
      implicit val pid1 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes = createEnvelopes(pid1, 6)
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val bogusEventHandler = new TransactConcatHandler(_.sequenceNr == 4)

      val projectionFailing =
        DynamoDBProjection
          .exactlyOnce(projectionId, Some(settings), sourceProvider, handler = () => bogusEventHandler)
          .withRecoveryStrategy(HandlerRecoveryStrategy.skip)

      offsetShouldBeEmpty()
      projectionTestKit.run(projectionFailing) {
        projectedTestValueShouldBe("e1|e2|e3|e5|e6")
      }
      latestOffsetShouldBe(envelopes.last.offset)
    }

    "store offset for failing events when using RecoveryStrategy.skip" in {
      implicit val pid1 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes = createEnvelopes(pid1, 6)
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val bogusEventHandler = new TransactConcatHandler(_.sequenceNr == 6)

      val projectionFailing =
        DynamoDBProjection
          .exactlyOnce(projectionId, Some(settings), sourceProvider, handler = () => bogusEventHandler)
          .withRecoveryStrategy(HandlerRecoveryStrategy.skip)

      offsetShouldBeEmpty()
      projectionTestKit.run(projectionFailing) {
        projectedTestValueShouldBe("e1|e2|e3|e4|e5")
        bogusEventHandler.attempts shouldBe 1
      }
      eventually {
        latestOffsetShouldBe(envelopes.last.offset)
      }
    }

    "skip failing events after retrying when using RecoveryStrategy.retryAndSkip" in {
      implicit val pid1 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes = createEnvelopes(pid1, 6)
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val statusProbe = createTestProbe[TestStatusObserver.Status]()
      val progressProbe = createTestProbe[TestStatusObserver.OffsetProgress[Envelope]]()
      val statusObserver = new DynamoDBTestStatusObserver(statusProbe.ref, progressProbe.ref)
      val bogusEventHandler = new TransactConcatHandler(_.sequenceNr == 4)

      val projectionFailing =
        DynamoDBProjection
          .exactlyOnce(projectionId, Some(settings), sourceProvider, handler = () => bogusEventHandler)
          .withRecoveryStrategy(HandlerRecoveryStrategy.retryAndSkip(3, 10.millis))
          .withStatusObserver(statusObserver)

      offsetShouldBeEmpty()
      projectionTestKit.run(projectionFailing) {
        projectedTestValueShouldBe("e1|e2|e3|e5|e6")
      }

      // 1 + 3 => 1 original attempt and 3 retries
      bogusEventHandler.attempts shouldBe 1 + 3

      val someTestException = TestException("err")
      statusProbe.expectMessage(TestStatusObserver.Err(Envelope(pid1, 4, "e4"), someTestException))
      statusProbe.expectMessage(TestStatusObserver.Err(Envelope(pid1, 4, "e4"), someTestException))
      statusProbe.expectMessage(TestStatusObserver.Err(Envelope(pid1, 4, "e4"), someTestException))
      statusProbe.expectMessage(TestStatusObserver.Err(Envelope(pid1, 4, "e4"), someTestException))
      statusProbe.expectNoMessage()
      progressProbe.expectMessage(TestStatusObserver.OffsetProgress(Envelope(pid1, 1, "e1")))
      progressProbe.expectMessage(TestStatusObserver.OffsetProgress(Envelope(pid1, 2, "e2")))
      progressProbe.expectMessage(TestStatusObserver.OffsetProgress(Envelope(pid1, 3, "e3")))
      // Offset 4 is stored even though it failed and was skipped
      progressProbe.expectMessage(TestStatusObserver.OffsetProgress(Envelope(pid1, 4, "e4")))
      progressProbe.expectMessage(TestStatusObserver.OffsetProgress(Envelope(pid1, 5, "e5")))
      progressProbe.expectMessage(TestStatusObserver.OffsetProgress(Envelope(pid1, 6, "e6")))

      latestOffsetShouldBe(envelopes.last.offset)
    }

    "fail after retrying when using RecoveryStrategy.retryAndFail" in {
      implicit val pid1 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes = createEnvelopes(pid1, 6)
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val bogusEventHandler = new TransactConcatHandler(_.sequenceNr == 4)

      val projectionFailing =
        DynamoDBProjection
          .exactlyOnce(projectionId, Some(settings), sourceProvider, handler = () => bogusEventHandler)
          .withRecoveryStrategy(HandlerRecoveryStrategy.retryAndFail(3, 10.millis))

      offsetShouldBeEmpty()
      projectionTestKit.runWithTestSink(projectionFailing) { sinkProbe =>
        sinkProbe.request(1000)
        eventuallyExpectError(sinkProbe).getMessage should startWith(concatHandlerFail4Msg)
      }
      projectedTestValueShouldBe("e1|e2|e3")
      // 1 + 3 => 1 original attempt and 3 retries
      bogusEventHandler.attempts shouldBe 1 + 3

      latestOffsetShouldBe(envelopes(2).offset) // <- offset is from e3
    }

    "restart from previous offset - fail with throwing an exception" in {
      implicit val pid = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes = createEnvelopes(pid, 6)
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      def exactlyOnceProjection(failWhen: EventEnvelope[String] => Boolean = _ => false) = {
        DynamoDBProjection.exactlyOnce(
          projectionId,
          Some(settings),
          sourceProvider,
          handler = () => new TransactConcatHandler(failWhen))
      }

      offsetShouldBeEmpty()
      projectionTestKit.runWithTestSink(exactlyOnceProjection(_.sequenceNr == 4)) { sinkProbe =>
        sinkProbe.request(1000)
        eventuallyExpectError(sinkProbe).getMessage should startWith(concatHandlerFail4Msg)
      }
      projectedTestValueShouldBe("e1|e2|e3")
      latestOffsetShouldBe(envelopes(2).offset)

      // re-run projection without failing function
      projectionTestKit.run(exactlyOnceProjection()) {
        projectedTestValueShouldBe("e1|e2|e3|e4|e5|e6")
      }
      latestOffsetShouldBe(envelopes.last.offset)
    }

    "filter out duplicates" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes = createEnvelopesWithDuplicates(pid1, pid2)
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val projection =
        DynamoDBProjection.exactlyOnce(
          projectionId,
          Some(settings),
          sourceProvider,
          handler = () => new TransactConcatHandler)

      projectionTestKit.run(projection) {
        projectedTestValueShouldBe("e1-1|e1-2|e1-3|e1-4")(pid1)
        projectedTestValueShouldBe("e2-1|e2-2|e2-3")(pid2)
      }
      latestOffsetShouldBe(envelopes.last.offset)
    }

    "filter out unknown sequence numbers" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val startTime = TestClock.nowMicros().instant()
      val envelopes1 = createEnvelopesUnknownSequenceNumbers(startTime, pid1, pid2)
      val sourceProvider1 = createSourceProvider(envelopes1)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider1)

      val projection1 =
        DynamoDBProjection.exactlyOnce(
          projectionId,
          Some(settings),
          sourceProvider1,
          handler = () => new TransactConcatHandler)

      projectionTestKit.run(projection1) {
        projectedTestValueShouldBe("e1-1|e1-2|e1-3")(pid1)
        projectedTestValueShouldBe("e2-1")(pid2)
      }
      latestOffsetShouldBe(envelopes1.collectFirst { case env if env.event == "e1-3" => env.offset }.get)

      // simulate backtracking
      logger.debug("Starting backtracking")
      val envelopes2 = createEnvelopesBacktrackingUnknownSequenceNumbers(startTime, pid1, pid2)
      val sourceProvider2 = createBacktrackingSourceProvider(envelopes2)
      val projection2 =
        DynamoDBProjection.exactlyOnce(
          projectionId,
          Some(settings),
          sourceProvider2,
          handler = () => new TransactConcatHandler)

      projectionTestKit.run(projection2) {
        projectedTestValueShouldBe("e1-1|e1-2|e1-3|e1-4|e1-5|e1-6")(pid1)
        projectedTestValueShouldBe("e2-1|e2-2|e2-3|e2-4")(pid2)
      }
      latestOffsetShouldBe(envelopes2.last.offset)
    }

    "be able to skip envelopes but still store offset" in {
      implicit val pid = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val envelopes = createEnvelopes(pid, 6).map { env =>
        if (env.event == "e3" || env.event == "e4" || env.event == "e6")
          markAsFilteredEvent(env)
        else
          env
      }
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val projection =
        DynamoDBProjection.exactlyOnce(
          projectionId,
          Some(settings),
          sourceProvider,
          handler = () => new TransactConcatHandler)

      offsetShouldBeEmpty()
      projectionTestKit.run(projection) {
        projectedTestValueShouldBe("e1|e2|e5")
        offsetStore.storedSeqNr(pid).futureValue shouldBe 6
      }
      latestOffsetShouldBe(envelopes.last.offset)
    }

    "replay rejected sequence numbers" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val allEnvelopes = createEnvelopes(pid1, 6) ++ createEnvelopes(pid2, 3)
      val skipPid1SeqNrs = Set(3L, 4L, 5L)
      val envelopes = allEnvelopes.filterNot { env =>
        (env.persistenceId == pid1 && skipPid1SeqNrs(env.sequenceNr)) ||
        (env.persistenceId == pid2 && (env.sequenceNr == 1))
      }

      val sourceProvider =
        createSourceProviderWithMoreEnvelopes(envelopes, allEnvelopes, enableCurrentEventsByPersistenceId = true)

      implicit val offsetStore: DynamoDBOffsetStore =
        new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)

      val projectionRef = spawn(
        ProjectionBehavior(
          DynamoDBProjection
            .exactlyOnce(
              projectionId,
              Some(settings.withReplayOnRejectedSequenceNumbers(true)),
              sourceProvider,
              handler = () => new TransactConcatHandler)))

      eventually {
        projectedTestValueShouldBe("e1|e2|e3|e4|e5|e6")(pid1)
        projectedTestValueShouldBe("e1|e2|e3")(pid2)
      }

      eventually {
        latestOffsetShouldBe(allEnvelopes.last.offset)
      }
      projectionRef ! ProjectionBehavior.Stop
    }

    "replay rejected sequence numbers due to clock skew on event write" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val start = tick().instant()

      def createEnvelopesFor(
          pid: Pid,
          fromSeqNr: Int,
          toSeqNr: Int,
          fromTimestamp: Instant): immutable.IndexedSeq[EventEnvelope[String]] = {
        (fromSeqNr to toSeqNr).map { n =>
          createEnvelope(pid, n, fromTimestamp.plusSeconds(n - fromSeqNr), s"e$n")
        }
      }

      val envelopes1 =
        createEnvelopesFor(pid1, 1, 2, start) ++
        createEnvelopesFor(pid1, 3, 4, start.plusSeconds(4)) ++ // gap
        createEnvelopesFor(pid1, 5, 9, start.plusSeconds(2)) // clock skew, back 2, and then overlapping

      val envelopes2 =
        createEnvelopesFor(pid2, 1, 3, start.plusSeconds(10)) ++
        createEnvelopesFor(pid2, 4, 6, start.plusSeconds(1)) ++ // clock skew, back 9
        createEnvelopesFor(pid2, 7, 9, start.plusSeconds(20)) // and gap

      val allEnvelopes = envelopes1 ++ envelopes2

      val envelopes = allEnvelopes.sortBy(_.timestamp)

      val sourceProvider =
        createSourceProviderWithMoreEnvelopes(envelopes, allEnvelopes, enableCurrentEventsByPersistenceId = true)

      implicit val offsetStore: DynamoDBOffsetStore =
        new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)

      val projectionRef = spawn(
        ProjectionBehavior(
          DynamoDBProjection
            .exactlyOnce(
              projectionId,
              Some(settings.withReplayOnRejectedSequenceNumbers(true)),
              sourceProvider,
              handler = () => new TransactConcatHandler)))

      eventually {
        projectedTestValueShouldBe("e1|e2|e3|e4|e5|e6|e7|e8|e9")(pid1)
        projectedTestValueShouldBe("e1|e2|e3|e4|e5|e6|e7|e8|e9")(pid2)
      }

      eventually {
        latestOffsetShouldBe(envelopes.last.offset)
      }

      projectionRef ! ProjectionBehavior.Stop
    }

    "accept events after detecting sequence number reset for exactly-once" in {
      val testSettings = settings.withAcceptSequenceNumberResetAfter(10.seconds)

      val pid1 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val start = tick().instant()

      def createEnvelopesFor(
          pid: Pid,
          fromSeqNr: Int,
          toSeqNr: Int,
          fromTimestamp: Instant): immutable.IndexedSeq[EventEnvelope[String]] = {
        (fromSeqNr to toSeqNr).map { n =>
          createEnvelope(pid, n, fromTimestamp.plusSeconds(n - fromSeqNr), s"e$n")
        }
      }

      val envelopes =
        createEnvelopesFor(pid1, 1, 5, start) ++
        createEnvelopesFor(pid1, 1, 3, start.plusSeconds(30))

      val sourceProvider = createSourceProvider(envelopes)

      implicit val offsetStore: DynamoDBOffsetStore =
        new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)

      val projectionRef = spawn(
        ProjectionBehavior(DynamoDBProjection
          .exactlyOnce(projectionId, Some(testSettings), sourceProvider, handler = () => new TransactConcatHandler)))

      eventually {
        projectedTestValueShouldBe("e1|e2|e3|e4|e5|e1|e2|e3")(pid1)
      }

      eventually {
        latestOffsetShouldBe(envelopes.last.offset)
      }

      projectionRef ! ProjectionBehavior.Stop
    }

    "replay rejected sequence numbers after detecting reset for exactly-once" in {
      val testSettings = settings.withAcceptSequenceNumberResetAfter(10.seconds)

      val pid1 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val start = tick().instant()

      def createEnvelopesFor(
          pid: Pid,
          fromSeqNr: Int,
          toSeqNr: Int,
          fromTimestamp: Instant): immutable.IndexedSeq[EventEnvelope[String]] = {
        (fromSeqNr to toSeqNr).map { n =>
          createEnvelope(pid, n, fromTimestamp.plusSeconds(n - fromSeqNr), s"e$n")
        }
      }

      val envelopes1 = createEnvelopesFor(pid1, 1, 5, start)

      val envelopes2 = createEnvelopesFor(pid1, 1, 5, start.plusSeconds(30))

      val allEnvelopes = envelopes1 ++ envelopes2

      val envelopes = envelopes1 ++ envelopes2.drop(2) // start from seq nr 3 after reset

      val sourceProvider =
        createSourceProviderWithMoreEnvelopes(envelopes, allEnvelopes, enableCurrentEventsByPersistenceId = true)

      implicit val offsetStore: DynamoDBOffsetStore =
        new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)

      val projectionRef = spawn(
        ProjectionBehavior(DynamoDBProjection
          .exactlyOnce(projectionId, Some(testSettings), sourceProvider, handler = () => new TransactConcatHandler)))

      eventually {
        projectedTestValueShouldBe("e1|e2|e3|e4|e5|e1|e2|e3|e4|e5")(pid1)
      }

      eventually {
        latestOffsetShouldBe(envelopes.last.offset)
      }

      projectionRef ! ProjectionBehavior.Stop
    }
  }

  "A DynamoDB grouped projection with TimestampOffset" must {
    "persist projection and offset in the same write operation (transactional)" in {
      implicit val pid = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes = createEnvelopes(pid, 6)
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val handlerProbe = createTestProbe[String]("calls-to-handler")

      val projection =
        DynamoDBProjection
          .exactlyOnceGroupedWithin(
            projectionId,
            Some(settings),
            sourceProvider,
            handler = () => new TransactGroupedConcatHandler(handlerProbe.ref))
          .withGroup(3, 3.seconds)

      offsetShouldBeEmpty()
      projectionTestKit.run(projection) {
        projectedTestValueShouldBe("e1|e2|e3|e4|e5|e6")
      }

      // handler probe is called twice
      handlerProbe.expectMessage("called")
      handlerProbe.expectMessage("called")

      latestOffsetShouldBe(envelopes.last.offset)
    }

    "filter duplicates" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes = createEnvelopesWithDuplicates(pid1, pid2)
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val handlerProbe = createTestProbe[String]("calls-to-handler")

      val projection =
        DynamoDBProjection
          .exactlyOnceGroupedWithin(
            projectionId,
            Some(settings),
            sourceProvider,
            handler = () => new TransactGroupedConcatHandler(handlerProbe.ref))
          .withGroup(3, 3.seconds)

      projectionTestKit.run(projection) {
        projectedTestValueShouldBe("e1-1|e1-2|e1-3|e1-4")(pid1)
        projectedTestValueShouldBe("e2-1|e2-2|e2-3")(pid2)
      }

      // handler probe is called twice
      handlerProbe.expectMessage("called")
      handlerProbe.expectMessage("called")

      latestOffsetShouldBe(envelopes.last.offset)
    }

    "filter out unknown sequence numbers" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val startTime = TestClock.nowMicros().instant()
      val envelopes1 = createEnvelopesUnknownSequenceNumbers(startTime, pid1, pid2)
      val sourceProvider1 = createBacktrackingSourceProvider(envelopes1)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider1)

      val handlerProbe = createTestProbe[String]()

      val projection1 =
        DynamoDBProjection
          .exactlyOnceGroupedWithin(
            projectionId,
            Some(settings),
            sourceProvider1,
            handler = () => new TransactGroupedConcatHandler(handlerProbe.ref))
          .withGroup(3, 3.seconds)

      projectionTestKit.run(projection1) {
        projectedTestValueShouldBe("e1-1|e1-2|e1-3")(pid1)
        projectedTestValueShouldBe("e2-1")(pid2)
      }
      latestOffsetShouldBe(envelopes1.collectFirst { case env if env.event == "e1-3" => env.offset }.get)

      // simulate backtracking
      logger.debug("Starting backtracking")
      val envelopes2 = createEnvelopesBacktrackingUnknownSequenceNumbers(startTime, pid1, pid2)
      val sourceProvider2 = createBacktrackingSourceProvider(envelopes2)
      val projection2 =
        DynamoDBProjection
          .exactlyOnceGroupedWithin(
            projectionId,
            Some(settings),
            sourceProvider2,
            handler = () => new TransactGroupedConcatHandler(handlerProbe.ref))
          .withGroup(3, 3.seconds)

      projectionTestKit.run(projection2) {
        projectedTestValueShouldBe("e1-1|e1-2|e1-3|e1-4|e1-5|e1-6")(pid1)
        projectedTestValueShouldBe("e2-1|e2-2|e2-3|e2-4")(pid2)
      }
      latestOffsetShouldBe(envelopes2.last.offset)
    }

    "be able to skip envelopes but still store offset" in {
      implicit val pid = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes = createEnvelopes(pid, 6).map { env =>
        if (env.event == "e3" || env.event == "e4" || env.event == "e6")
          markAsFilteredEvent(env)
        else
          env
      }
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val handlerProbe = createTestProbe[String]("calls-to-handler")

      val projection =
        DynamoDBProjection
          .exactlyOnceGroupedWithin(
            projectionId,
            Some(settings),
            sourceProvider,
            handler = () => new TransactGroupedConcatHandler(handlerProbe.ref))
          .withGroup(3, 3.seconds)

      offsetShouldBeEmpty()
      projectionTestKit.run(projection) {
        projectedTestValueShouldBe("e1|e2|e5")
      }

      // handler probe is called twice
      handlerProbe.expectMessage("called")
      handlerProbe.expectMessage("called")

      latestOffsetShouldBe(envelopes.last.offset)
    }

    "replay rejected sequence numbers for exactly-once grouped" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val allEnvelopes = createEnvelopes(pid1, 10) ++ createEnvelopes(pid2, 3)
      val skipPid1SeqNrs = Set(3L, 4L, 5L, 7L, 9L)
      val envelopes = allEnvelopes.filterNot { env =>
        (env.persistenceId == pid1 && skipPid1SeqNrs(env.sequenceNr)) ||
        (env.persistenceId == pid2 && (env.sequenceNr == 1))
      }

      val sourceProvider =
        createSourceProviderWithMoreEnvelopes(envelopes, allEnvelopes, enableCurrentEventsByPersistenceId = true)

      implicit val offsetStore: DynamoDBOffsetStore =
        new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)

      val handlerProbe = createTestProbe[String]("calls-to-handler")

      val projectionRef = spawn(
        ProjectionBehavior(
          DynamoDBProjection
            .exactlyOnceGroupedWithin(
              projectionId,
              Some(settings.withReplayOnRejectedSequenceNumbers(true)),
              sourceProvider,
              handler = () => new TransactGroupedConcatHandler(handlerProbe.ref))
            .withGroup(8, 3.seconds)))

      eventually {
        projectedTestValueShouldBe("e1|e2|e3|e4|e5|e6|e7|e8|e9|e10")(pid1)
        projectedTestValueShouldBe("e1|e2|e3")(pid2)
      }

      eventually {
        latestOffsetShouldBe(allEnvelopes.last.offset)
      }
      projectionRef ! ProjectionBehavior.Stop
    }

    "replay rejected sequence numbers due to clock skew on event write for exactly-once grouped" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val start = tick().instant()

      def createEnvelopesFor(
          pid: Pid,
          fromSeqNr: Int,
          toSeqNr: Int,
          fromTimestamp: Instant): immutable.IndexedSeq[EventEnvelope[String]] = {
        (fromSeqNr to toSeqNr).map { n =>
          createEnvelope(pid, n, fromTimestamp.plusSeconds(n - fromSeqNr), s"e$n")
        }
      }

      val envelopes1 =
        createEnvelopesFor(pid1, 1, 2, start) ++
        createEnvelopesFor(pid1, 3, 4, start.plusSeconds(4)) ++ // gap
        createEnvelopesFor(pid1, 5, 9, start.plusSeconds(2)) // clock skew, back 2, and then overlapping

      val envelopes2 =
        createEnvelopesFor(pid2, 1, 3, start.plusSeconds(10)) ++
        createEnvelopesFor(pid2, 4, 6, start.plusSeconds(1)) ++ // clock skew, back 9
        createEnvelopesFor(pid2, 7, 9, start.plusSeconds(20)) // and gap

      val allEnvelopes = envelopes1 ++ envelopes2

      val envelopes = allEnvelopes.sortBy(_.timestamp)

      val sourceProvider =
        createSourceProviderWithMoreEnvelopes(envelopes, allEnvelopes, enableCurrentEventsByPersistenceId = true)

      implicit val offsetStore: DynamoDBOffsetStore =
        new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)

      val handlerProbe = createTestProbe[String]("calls-to-handler")

      val projectionRef = spawn(
        ProjectionBehavior(
          DynamoDBProjection
            .exactlyOnceGroupedWithin(
              projectionId,
              Some(settings.withReplayOnRejectedSequenceNumbers(true)),
              sourceProvider,
              handler = () => new TransactGroupedConcatHandler(handlerProbe.ref))
            .withGroup(8, 3.seconds)))

      eventually {
        projectedTestValueShouldBe("e1|e2|e3|e4|e5|e6|e7|e8|e9")(pid1)
        projectedTestValueShouldBe("e1|e2|e3|e4|e5|e6|e7|e8|e9")(pid2)
      }

      eventually {
        latestOffsetShouldBe(allEnvelopes.last.offset)
      }
      projectionRef ! ProjectionBehavior.Stop
    }

    "accept events after detecting sequence number reset for exactly-once grouped" in {
      val testSettings = settings.withAcceptSequenceNumberResetAfter(10.seconds)

      val pid1 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val start = tick().instant()

      def createEnvelopesFor(
          pid: Pid,
          fromSeqNr: Int,
          toSeqNr: Int,
          fromTimestamp: Instant): immutable.IndexedSeq[EventEnvelope[String]] = {
        (fromSeqNr to toSeqNr).map { n =>
          createEnvelope(pid, n, fromTimestamp.plusSeconds(n - fromSeqNr), s"e$n")
        }
      }

      val envelopes1 = createEnvelopesFor(pid1, 1, 5, start)
      val envelopes2 = createEnvelopesFor(pid1, 1, 3, start.plusSeconds(30))
      val allEnvelopes = envelopes1 ++ envelopes2

      val (sourceProvider, sourceProbe) = createDynamicSourceProvider(allEnvelopes)

      implicit val offsetStore: DynamoDBOffsetStore =
        new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)

      val handlerProbe = createTestProbe[String]("calls-to-handler")

      val projectionRef = spawn(
        ProjectionBehavior(
          DynamoDBProjection
            .exactlyOnceGroupedWithin(
              projectionId,
              Some(testSettings),
              sourceProvider,
              handler = () => new TransactGroupedConcatHandler(handlerProbe.ref))
            .withGroup(8, 3.seconds)))

      val source = sourceProbe.futureValue

      envelopes1.foreach(source.sendNext)

      eventually {
        projectedTestValueShouldBe("e1|e2|e3|e4|e5")(pid1)
        latestOffsetShouldBe(envelopes1.last.offset)
      }

      envelopes2.foreach(source.sendNext)

      eventually {
        projectedTestValueShouldBe("e1|e2|e3|e4|e5|e1|e2|e3")(pid1)
        latestOffsetShouldBe(envelopes2.last.offset)
      }

      projectionRef ! ProjectionBehavior.Stop
    }

    "replay rejected sequence numbers after detecting reset for exactly-once grouped" in {
      val testSettings = settings.withAcceptSequenceNumberResetAfter(10.seconds)

      val pid1 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val start = tick().instant()

      def createEnvelopesFor(
          pid: Pid,
          fromSeqNr: Int,
          toSeqNr: Int,
          fromTimestamp: Instant): immutable.IndexedSeq[EventEnvelope[String]] = {
        (fromSeqNr to toSeqNr).map { n =>
          createEnvelope(pid, n, fromTimestamp.plusSeconds(n - fromSeqNr), s"e$n")
        }
      }

      val envelopes1 = createEnvelopesFor(pid1, 1, 5, start)
      val envelopes2 = createEnvelopesFor(pid1, 1, 5, start.plusSeconds(30))
      val allEnvelopes = envelopes1 ++ envelopes2

      val (sourceProvider, sourceProbe) =
        createDynamicSourceProvider(allEnvelopes, enableCurrentEventsByPersistenceId = true)

      implicit val offsetStore: DynamoDBOffsetStore =
        new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)

      val handlerProbe = createTestProbe[String]("calls-to-handler")

      val projectionRef = spawn(
        ProjectionBehavior(
          DynamoDBProjection
            .exactlyOnceGroupedWithin(
              projectionId,
              Some(testSettings),
              sourceProvider,
              handler = () => new TransactGroupedConcatHandler(handlerProbe.ref))
            .withGroup(8, 3.seconds)))

      val source = sourceProbe.futureValue

      envelopes1.foreach(source.sendNext)

      eventually {
        projectedTestValueShouldBe("e1|e2|e3|e4|e5")(pid1)
        latestOffsetShouldBe(envelopes1.last.offset)
      }

      // start from seq nr 3 after reset
      envelopes2.drop(2).foreach(source.sendNext)

      eventually {
        projectedTestValueShouldBe("e1|e2|e3|e4|e5|e1|e2|e3|e4|e5")(pid1)
        latestOffsetShouldBe(envelopes2.last.offset)
      }

      projectionRef ! ProjectionBehavior.Stop
    }

    "handle at-least-once grouped projection" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes1 = createEnvelopes(pid1, 3)
      val envelopes2 = createEnvelopes(pid2, 3)
      val envelopes = envelopes1.zip(envelopes2).flatMap { case (a, b) => List(a, b) }
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val result = new StringBuffer()

      def handler(): Handler[immutable.Seq[EventEnvelope[String]]] =
        new Handler[immutable.Seq[EventEnvelope[String]]] {
          override def process(envelopes: immutable.Seq[EventEnvelope[String]]): Future[Done] = {
            Future {
              envelopes.foreach(env => result.append(env.persistenceId).append("_").append(env.event).append("|"))
            }.map(_ => Done)
          }
        }

      val projection =
        DynamoDBProjection
          .atLeastOnceGroupedWithin(projectionId, Some(settings), sourceProvider, handler = () => handler())
          .withGroup(2, 3.seconds)

      offsetShouldBeEmpty()
      projectionTestKit.run(projection) {
        result.toString shouldBe s"${pid1}_e1|${pid2}_e1|${pid1}_e2|${pid2}_e2|${pid1}_e3|${pid2}_e3|"
      }

      eventually {
        latestOffsetShouldBe(envelopes.last.offset)
      }
      offsetStore.storedSeqNr(pid1).futureValue shouldBe 3
      offsetStore.storedSeqNr(pid2).futureValue shouldBe 3
    }

    "filter duplicates for at-least-once grouped projection" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes = createEnvelopesWithDuplicates(pid1, pid2)
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val result1 = new StringBuffer()
      val result2 = new StringBuffer()

      def handler(): Handler[immutable.Seq[EventEnvelope[String]]] = new Handler[immutable.Seq[EventEnvelope[String]]] {
        override def process(envelopes: immutable.Seq[EventEnvelope[String]]): Future[Done] = {
          Future
            .successful {
              envelopes.foreach { envelope =>
                if (envelope.persistenceId == pid1)
                  result1.append(envelope.event).append("|")
                else
                  result2.append(envelope.event).append("|")
              }
            }
            .map(_ => Done)
        }
      }

      val projection =
        DynamoDBProjection.atLeastOnceGroupedWithin(
          projectionId,
          Some(settings),
          sourceProvider,
          handler = () => handler())

      projectionTestKit.run(projection) {
        result1.toString shouldBe "e1-1|e1-2|e1-3|e1-4|"
        result2.toString shouldBe "e2-1|e2-2|e2-3|"
      }
      eventually {
        latestOffsetShouldBe(envelopes.last.offset)
      }
    }

    "filter out unknown sequence numbers for at-least-once grouped projection" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val startTime = TestClock.nowMicros().instant()
      val sourceProvider = new TestSourceProviderWithInput()
      implicit val offsetStore: DynamoDBOffsetStore =
        new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)

      val result1 = new StringBuffer()
      val result2 = new StringBuffer()

      def handler(): Handler[immutable.Seq[EventEnvelope[String]]] = new Handler[immutable.Seq[EventEnvelope[String]]] {
        override def process(envelopes: immutable.Seq[EventEnvelope[String]]): Future[Done] = {
          Future
            .successful {
              envelopes.foreach { envelope =>
                if (envelope.persistenceId == pid1)
                  result1.append(envelope.event).append("|")
                else
                  result2.append(envelope.event).append("|")
              }
            }
            .map(_ => Done)
        }
      }

      val projectionRef = spawn(
        ProjectionBehavior(DynamoDBProjection
          .atLeastOnceGroupedWithin(projectionId, Some(settings), sourceProvider, handler = () => handler())))
      val input = sourceProvider.input.futureValue

      val envelopes1 = createEnvelopesUnknownSequenceNumbers(startTime, pid1, pid2)
      envelopes1.foreach(input ! _)

      eventually {
        result1.toString shouldBe "e1-1|e1-2|e1-3|"
        result2.toString shouldBe "e2-1|"
      }

      // simulate backtracking
      logger.debug("Starting backtracking")
      val envelopes2 = createEnvelopesBacktrackingUnknownSequenceNumbers(startTime, pid1, pid2)
      envelopes2.foreach(input ! _)

      eventually {
        result1.toString shouldBe "e1-1|e1-2|e1-3|e1-4|e1-5|e1-6|"
        result2.toString shouldBe "e2-1|e2-2|e2-3|e2-4|"
      }

      eventually {
        latestOffsetShouldBe(envelopes2.last.offset)
      }
      projectionRef ! ProjectionBehavior.Stop
    }

    "be able to skip envelopes but still store offset for at-least-once grouped projection" in {
      implicit val pid = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes = createEnvelopes(pid, 6).map { env =>
        if (env.event == "e3" || env.event == "e4" || env.event == "e6")
          markAsFilteredEvent(env)
        else
          env
      }
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val result = new StringBuffer()

      def handler(): Handler[immutable.Seq[EventEnvelope[String]]] =
        new Handler[immutable.Seq[EventEnvelope[String]]] {
          override def process(envelopes: immutable.Seq[EventEnvelope[String]]): Future[Done] = {
            Future {
              envelopes.foreach(env => result.append(env.event).append("|"))
            }.map(_ => Done)
          }
        }

      val projection =
        DynamoDBProjection
          .atLeastOnceGroupedWithin(projectionId, Some(settings), sourceProvider, handler = () => handler())
          .withGroup(2, 3.seconds)

      offsetShouldBeEmpty()
      projectionTestKit.run(projection) {
        result.toString shouldBe "e1|e2|e5|"
      }

      eventually {
        latestOffsetShouldBe(envelopes.last.offset)
      }
    }

    "replay rejected sequence numbers for at-least-once grouped" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val allEnvelopes = createEnvelopes(pid1, 10) ++ createEnvelopes(pid2, 3)
      val skipPid1SeqNrs = Set(3L, 4L, 5L, 7L, 9L)
      val envelopes = allEnvelopes.filterNot { env =>
        (env.persistenceId == pid1 && skipPid1SeqNrs(env.sequenceNr)) ||
        (env.persistenceId == pid2 && (env.sequenceNr == 1))
      }

      val sourceProvider =
        createSourceProviderWithMoreEnvelopes(envelopes, allEnvelopes, enableCurrentEventsByPersistenceId = true)

      implicit val offsetStore: DynamoDBOffsetStore =
        new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)

      val results = new ConcurrentHashMap[String, String]()

      val handler: Handler[Seq[EventEnvelope[String]]] =
        (envelopes: Seq[EventEnvelope[String]]) => {
          Future {
            envelopes.foreach { envelope =>
              results.putIfAbsent(envelope.persistenceId, "|")
              results.computeIfPresent(envelope.persistenceId, (_, value) => value + envelope.event + "|")
            }
          }.map(_ => Done)
        }

      val projection =
        DynamoDBProjection
          .atLeastOnceGroupedWithin(
            projectionId,
            Some(settings.withReplayOnRejectedSequenceNumbers(true)),
            sourceProvider,
            handler = () => handler)
          .withGroup(8, 3.seconds)

      offsetShouldBeEmpty()

      projectionTestKit.run(projection) {
        results.get(pid1) shouldBe "|e1|e2|e3|e4|e5|e6|e7|e8|e9|e10|"
        results.get(pid2) shouldBe "|e1|e2|e3|"
      }

      eventually {
        latestOffsetShouldBe(allEnvelopes.last.offset)
      }
    }

    "replay rejected sequence numbers due to clock skew on event write for at-least-once grouped" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val start = tick().instant()

      def createEnvelopesFor(
          pid: Pid,
          fromSeqNr: Int,
          toSeqNr: Int,
          fromTimestamp: Instant): immutable.IndexedSeq[EventEnvelope[String]] = {
        (fromSeqNr to toSeqNr).map { n =>
          createEnvelope(pid, n, fromTimestamp.plusSeconds(n - fromSeqNr), s"e$n")
        }
      }

      val envelopes1 =
        createEnvelopesFor(pid1, 1, 2, start) ++
        createEnvelopesFor(pid1, 3, 4, start.plusSeconds(4)) ++ // gap
        createEnvelopesFor(pid1, 5, 9, start.plusSeconds(2)) // clock skew, back 2, and then overlapping

      val envelopes2 =
        createEnvelopesFor(pid2, 1, 3, start.plusSeconds(10)) ++
        createEnvelopesFor(pid2, 4, 6, start.plusSeconds(1)) ++ // clock skew, back 9
        createEnvelopesFor(pid2, 7, 9, start.plusSeconds(20)) // and gap

      val allEnvelopes = envelopes1 ++ envelopes2

      val envelopes = allEnvelopes.sortBy(_.timestamp)

      val sourceProvider =
        createSourceProviderWithMoreEnvelopes(envelopes, allEnvelopes, enableCurrentEventsByPersistenceId = true)

      implicit val offsetStore: DynamoDBOffsetStore =
        new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)

      val results = new ConcurrentHashMap[String, String]()

      val handler: Handler[Seq[EventEnvelope[String]]] =
        (envelopes: Seq[EventEnvelope[String]]) => {
          Future {
            envelopes.foreach { envelope =>
              results.putIfAbsent(envelope.persistenceId, "|")
              results.computeIfPresent(envelope.persistenceId, (_, value) => value + envelope.event + "|")
            }
          }.map(_ => Done)
        }

      val projection =
        DynamoDBProjection
          .atLeastOnceGroupedWithin(
            projectionId,
            Some(settings.withReplayOnRejectedSequenceNumbers(true)),
            sourceProvider,
            handler = () => handler)
          .withGroup(2, 3.seconds)

      offsetShouldBeEmpty()

      projectionTestKit.run(projection) {
        results.get(pid1) shouldBe "|e1|e2|e3|e4|e5|e6|e7|e8|e9|"
        results.get(pid2) shouldBe "|e1|e2|e3|e4|e5|e6|e7|e8|e9|"
      }

      eventually {
        latestOffsetShouldBe(allEnvelopes.last.offset)
      }
    }

    "replay rejected sequence numbers for at-least-once grouped (javadsl)" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val allEnvelopes = createEnvelopes(pid1, 10) ++ createEnvelopes(pid2, 3)
      val skipPid1SeqNrs = Set(3L, 4L, 5L, 7L, 9L)
      val envelopes = allEnvelopes.filterNot { env =>
        (env.persistenceId == pid1 && skipPid1SeqNrs(env.sequenceNr)) ||
        (env.persistenceId == pid2 && (env.sequenceNr == 1))
      }

      val sourceProvider =
        createJavaSourceProviderWithMoreEnvelopes(envelopes, allEnvelopes, enableCurrentEventsByPersistenceId = true)

      implicit val offsetStore: DynamoDBOffsetStore =
        new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)

      val results = new ConcurrentHashMap[String, String]()

      val handler: akka.projection.javadsl.Handler[java.util.List[EventEnvelope[String]]] =
        (envelopes: java.util.List[EventEnvelope[String]]) => {
          Future {
            envelopes.asScala.foreach { envelope =>
              results.putIfAbsent(envelope.persistenceId, "|")
              results.computeIfPresent(envelope.persistenceId, (_, value) => value + envelope.event + "|")
            }
          }.map(_ => Done.getInstance()).asJava
        }

      val projection =
        akka.projection.dynamodb.javadsl.DynamoDBProjection
          .atLeastOnceGroupedWithin(
            projectionId,
            Some(settings.withReplayOnRejectedSequenceNumbers(true)).toJava,
            sourceProvider,
            handler = () => handler,
            system)
          .withGroup(8, 3.seconds.toJava)

      offsetShouldBeEmpty()

      projectionTestKit.run(projection) {
        results.get(pid1) shouldBe "|e1|e2|e3|e4|e5|e6|e7|e8|e9|e10|"
        results.get(pid2) shouldBe "|e1|e2|e3|"
      }

      eventually {
        latestOffsetShouldBe(allEnvelopes.last.offset)
      }
    }

    "replay rejected sequence numbers due to clock skew on event write for at-least-once grouped (javadsl)" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val start = tick().instant()

      def createEnvelopesFor(
          pid: Pid,
          fromSeqNr: Int,
          toSeqNr: Int,
          fromTimestamp: Instant): immutable.IndexedSeq[EventEnvelope[String]] = {
        (fromSeqNr to toSeqNr).map { n =>
          createEnvelope(pid, n, fromTimestamp.plusSeconds(n - fromSeqNr), s"e$n")
        }
      }

      val envelopes1 =
        createEnvelopesFor(pid1, 1, 2, start) ++
        createEnvelopesFor(pid1, 3, 4, start.plusSeconds(4)) ++ // gap
        createEnvelopesFor(pid1, 5, 9, start.plusSeconds(2)) // clock skew, back 2, and then overlapping

      val envelopes2 =
        createEnvelopesFor(pid2, 1, 3, start.plusSeconds(10)) ++
        createEnvelopesFor(pid2, 4, 6, start.plusSeconds(1)) ++ // clock skew, back 9
        createEnvelopesFor(pid2, 7, 9, start.plusSeconds(20)) // and gap

      val allEnvelopes = envelopes1 ++ envelopes2

      val envelopes = allEnvelopes.sortBy(_.timestamp)

      val sourceProvider =
        createJavaSourceProviderWithMoreEnvelopes(envelopes, allEnvelopes, enableCurrentEventsByPersistenceId = true)

      implicit val offsetStore: DynamoDBOffsetStore =
        new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)

      val results = new ConcurrentHashMap[String, String]()

      val handler: akka.projection.javadsl.Handler[java.util.List[EventEnvelope[String]]] =
        (envelopes: java.util.List[EventEnvelope[String]]) => {
          Future {
            envelopes.asScala.foreach { envelope =>
              results.putIfAbsent(envelope.persistenceId, "|")
              results.computeIfPresent(envelope.persistenceId, (_, value) => value + envelope.event + "|")
            }
          }.map(_ => Done.getInstance()).asJava
        }

      val projection =
        akka.projection.dynamodb.javadsl.DynamoDBProjection
          .atLeastOnceGroupedWithin(
            projectionId,
            Some(settings.withReplayOnRejectedSequenceNumbers(true)).toJava,
            sourceProvider,
            handler = () => handler,
            system)
          .withGroup(2, 3.seconds.toJava)

      offsetShouldBeEmpty()

      projectionTestKit.run(projection) {
        results.get(pid1) shouldBe "|e1|e2|e3|e4|e5|e6|e7|e8|e9|"
        results.get(pid2) shouldBe "|e1|e2|e3|e4|e5|e6|e7|e8|e9|"
      }

      eventually {
        latestOffsetShouldBe(allEnvelopes.last.offset)
      }
    }

    "accept events after detecting sequence number reset for at-least-once grouped" in {
      val testSettings = settings.withAcceptSequenceNumberResetAfter(10.seconds)

      val pid1 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val start = tick().instant()

      def createEnvelopesFor(
          pid: Pid,
          fromSeqNr: Int,
          toSeqNr: Int,
          fromTimestamp: Instant): immutable.IndexedSeq[EventEnvelope[String]] = {
        (fromSeqNr to toSeqNr).map { n =>
          createEnvelope(pid, n, fromTimestamp.plusSeconds(n - fromSeqNr), s"e$n")
        }
      }

      val envelopes1 = createEnvelopesFor(pid1, 1, 5, start)
      val envelopes2 = createEnvelopesFor(pid1, 1, 3, start.plusSeconds(30))
      val allEnvelopes = envelopes1 ++ envelopes2

      val (sourceProvider, sourceProbe) = createDynamicSourceProvider(allEnvelopes)

      implicit val offsetStore: DynamoDBOffsetStore =
        new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)

      val results = new ConcurrentHashMap[String, String]()

      val handler: Handler[Seq[EventEnvelope[String]]] =
        (envelopes: Seq[EventEnvelope[String]]) => {
          Future {
            envelopes.foreach { envelope =>
              results.putIfAbsent(envelope.persistenceId, "|")
              results.computeIfPresent(envelope.persistenceId, (_, value) => value + envelope.event + "|")
            }
          }.map(_ => Done)
        }

      val projectionRef = spawn(
        ProjectionBehavior(
          DynamoDBProjection
            .atLeastOnceGroupedWithin(projectionId, Some(testSettings), sourceProvider, handler = () => handler)
            .withGroup(2, 3.seconds)))

      val source = sourceProbe.futureValue

      envelopes1.foreach(source.sendNext)

      eventually {
        results.get(pid1) shouldBe "|e1|e2|e3|e4|e5|"
        latestOffsetShouldBe(envelopes1.last.offset)
      }

      envelopes2.foreach(source.sendNext)

      eventually {
        results.get(pid1) shouldBe "|e1|e2|e3|e4|e5|e1|e2|e3|"
        latestOffsetShouldBe(envelopes2.last.offset)
      }

      projectionRef ! ProjectionBehavior.Stop
    }

    "replay rejected sequence numbers after detecting reset for at-least-once grouped" in {
      val testSettings = settings.withAcceptSequenceNumberResetAfter(10.seconds)

      val pid1 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val start = tick().instant()

      def createEnvelopesFor(
          pid: Pid,
          fromSeqNr: Int,
          toSeqNr: Int,
          fromTimestamp: Instant): immutable.IndexedSeq[EventEnvelope[String]] = {
        (fromSeqNr to toSeqNr).map { n =>
          createEnvelope(pid, n, fromTimestamp.plusSeconds(n - fromSeqNr), s"e$n")
        }
      }

      val envelopes1 = createEnvelopesFor(pid1, 1, 5, start)
      val envelopes2 = createEnvelopesFor(pid1, 1, 5, start.plusSeconds(30))
      val allEnvelopes = envelopes1 ++ envelopes2

      val (sourceProvider, sourceProbe) =
        createDynamicSourceProvider(allEnvelopes, enableCurrentEventsByPersistenceId = true)

      implicit val offsetStore: DynamoDBOffsetStore =
        new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)

      val results = new ConcurrentHashMap[String, String]()

      val handler: Handler[Seq[EventEnvelope[String]]] =
        (envelopes: Seq[EventEnvelope[String]]) => {
          Future {
            envelopes.foreach { envelope =>
              results.putIfAbsent(envelope.persistenceId, "|")
              results.computeIfPresent(envelope.persistenceId, (_, value) => value + envelope.event + "|")
            }
          }.map(_ => Done)
        }

      val projectionRef = spawn(
        ProjectionBehavior(
          DynamoDBProjection
            .atLeastOnceGroupedWithin(projectionId, Some(testSettings), sourceProvider, handler = () => handler)
            .withGroup(2, 3.seconds)))

      val source = sourceProbe.futureValue

      envelopes1.foreach(source.sendNext)

      eventually {
        results.get(pid1) shouldBe "|e1|e2|e3|e4|e5|"
        latestOffsetShouldBe(envelopes1.last.offset)
      }

      // start from seq nr 3 after reset
      envelopes2.drop(2).foreach(source.sendNext)

      eventually {
        results.get(pid1) shouldBe "|e1|e2|e3|e4|e5|e1|e2|e3|e4|e5|"
        latestOffsetShouldBe(envelopes2.last.offset)
      }

      projectionRef ! ProjectionBehavior.Stop
    }
  }

  "A DynamoDB flow projection with TimestampOffset" must {

    "persist projection and offset" in {
      implicit val pid = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes = createEnvelopes(pid, 6)
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      offsetShouldBeEmpty()

      val flowHandler =
        FlowWithContext[EventEnvelope[String], ProjectionContext]
          .mapAsync(1) { env =>
            repository.concatToText(env.persistenceId, env.event)
          }

      val projection =
        DynamoDBProjection
          .atLeastOnceFlow(projectionId, Some(settings), sourceProvider, handler = flowHandler)
          .withSaveOffset(1, 1.minute)

      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6")
      }
      eventually {
        latestOffsetShouldBe(envelopes.last.offset)
      }
    }

    "filter duplicates" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes = createEnvelopesWithDuplicates(pid1, pid2)
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      info(s"pid1 [$pid1], pid2 [$pid2]")

      val flowHandler =
        FlowWithContext[EventEnvelope[String], ProjectionContext]
          .mapAsync(1) { env =>
            repository.concatToText(env.persistenceId, env.event)
          }

      val projection =
        DynamoDBProjection
          .atLeastOnceFlow(projectionId, Some(settings), sourceProvider, handler = flowHandler)
          .withSaveOffset(2, 1.minute)

      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1-1|e1-2|e1-3|e1-4")(pid1)
        projectedValueShouldBe("e2-1|e2-2|e2-3")(pid2)
      }
      eventually {
        latestOffsetShouldBe(envelopes.last.offset)
      }
    }

    "filter out unknown sequence numbers" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val startTime = TestClock.nowMicros().instant()
      val sourceProvider = new TestSourceProviderWithInput()
      implicit val offsetStore: DynamoDBOffsetStore =
        new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)

      val flowHandler =
        FlowWithContext[EventEnvelope[String], ProjectionContext]
          .mapAsync(1) { env =>
            repository.concatToText(env.persistenceId, env.event)
          }

      val projectionRef = spawn(
        ProjectionBehavior(
          DynamoDBProjection
            .atLeastOnceFlow(projectionId, Some(settings), sourceProvider, handler = flowHandler)
            .withSaveOffset(2, 1.minute)))
      val input = sourceProvider.input.futureValue

      val envelopes1 = createEnvelopesUnknownSequenceNumbers(startTime, pid1, pid2)
      envelopes1.foreach(input ! _)

      eventually {
        projectedValueShouldBe("e1-1|e1-2|e1-3")(pid1)
        projectedValueShouldBe("e2-1")(pid2)
      }

      // simulate backtracking
      logger.debug("Starting backtracking")
      val envelopes2 = createEnvelopesBacktrackingUnknownSequenceNumbers(startTime, pid1, pid2)
      envelopes2.foreach(input ! _)

      eventually {
        projectedValueShouldBe("e1-1|e1-2|e1-3|e1-4|e1-5|e1-6")(pid1)
        projectedValueShouldBe("e2-1|e2-2|e2-3|e2-4")(pid2)
      }

      eventually {
        latestOffsetShouldBe(envelopes2.last.offset)
      }
      projectionRef ! ProjectionBehavior.Stop
    }

    "not support skipping envelopes but still store offset" in {
      // This is a limitation for atLeastOnceFlow and this test is just
      // capturing current behavior of not supporting the feature of skipping
      // envelopes that are marked with NotUsed in the eventMetadata.
      implicit val pid = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes = createEnvelopes(pid, 6).map { env =>
        if (env.event == "e3" || env.event == "e4" || env.event == "e6")
          markAsFilteredEvent(env)
        else
          env
      }
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      offsetShouldBeEmpty()

      val flowHandler =
        FlowWithContext[EventEnvelope[String], ProjectionContext]
          .mapAsync(1) { env =>
            repository.concatToText(env.persistenceId, env.event)
          }

      val projection =
        DynamoDBProjection
          .atLeastOnceFlow(projectionId, Some(settings), sourceProvider, handler = flowHandler)
          .withSaveOffset(1, 1.minute)

      projectionTestKit.run(projection) {
        // e3, e4, e6 are still included
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6")
      }
      eventually {
        latestOffsetShouldBe(envelopes.last.offset)
      }
    }

    "replay rejected sequence numbers for flow projection" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val slice1 = persistenceExt.sliceForPersistenceId(pid1)
      val slice2 = persistenceExt.sliceForPersistenceId(pid2)
      val projectionId = genRandomProjectionId()

      val pid1Envelopes = createEnvelopes(pid1, 6)
      val pid2Envelopes = createEnvelopes(pid2, 3)
      val allEnvelopes = pid1Envelopes ++ pid2Envelopes
      val skipPid1SeqNrs = Set(3L, 4L, 5L)
      val envelopes = allEnvelopes.filterNot { env =>
        (env.persistenceId == pid1 && skipPid1SeqNrs(env.sequenceNr)) ||
        (env.persistenceId == pid2 && (env.sequenceNr == 1))
      }

      val sourceProvider =
        createSourceProviderWithMoreEnvelopes(envelopes, allEnvelopes, enableCurrentEventsByPersistenceId = true)

      implicit val offsetStore: DynamoDBOffsetStore =
        new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)

      offsetShouldBeEmpty()

      val flowHandler =
        FlowWithContext[EventEnvelope[String], ProjectionContext]
          .mapAsync(1) { env =>
            repository.concatToText(env.persistenceId, env.event)
          }

      val projection =
        DynamoDBProjection
          .atLeastOnceFlow(
            projectionId,
            Some(settings.withReplayOnRejectedSequenceNumbers(true)),
            sourceProvider,
            handler = flowHandler)
          .withSaveOffset(1, 1.minute)

      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6")(pid1)
        projectedValueShouldBe("e1|e2|e3")(pid2)
      }

      eventually {
        latestOffsetShouldBe(allEnvelopes.last.offset)
        offsetStore.getState().offsetBySlice shouldBe Map(
          slice1 -> pid1Envelopes.last.offset,
          slice2 -> pid2Envelopes.last.offset)
      }
    }

    "replay rejected sequence numbers due to clock skew for flow projection" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val start = tick().instant()

      def createEnvelopesFor(
          pid: Pid,
          fromSeqNr: Int,
          toSeqNr: Int,
          fromTimestamp: Instant): immutable.IndexedSeq[EventEnvelope[String]] = {
        (fromSeqNr to toSeqNr).map { n =>
          createEnvelope(pid, n, fromTimestamp.plusSeconds(n - fromSeqNr), s"e$n")
        }
      }

      val envelopes1 =
        createEnvelopesFor(pid1, 1, 2, start) ++
        createEnvelopesFor(pid1, 3, 4, start.plusSeconds(4)) ++ // gap
        createEnvelopesFor(pid1, 5, 9, start.plusSeconds(2)) // clock skew, back 2, and then overlapping

      val envelopes2 =
        createEnvelopesFor(pid2, 1, 3, start.plusSeconds(10)) ++
        createEnvelopesFor(pid2, 4, 6, start.plusSeconds(1)) ++ // clock skew, back 9
        createEnvelopesFor(pid2, 7, 9, start.plusSeconds(20)) // and gap

      val allEnvelopes = envelopes1 ++ envelopes2

      val envelopes = allEnvelopes.sortBy(_.timestamp)
      val sourceProvider =
        createSourceProviderWithMoreEnvelopes(envelopes, allEnvelopes, enableCurrentEventsByPersistenceId = true)

      implicit val offsetStore: DynamoDBOffsetStore =
        new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)

      offsetShouldBeEmpty()

      val flowHandler =
        FlowWithContext[EventEnvelope[String], ProjectionContext]
          .mapAsync(1) { env =>
            repository.concatToText(env.persistenceId, env.event)
          }

      val projection =
        DynamoDBProjection
          .atLeastOnceFlow(
            projectionId,
            Some(settings.withReplayOnRejectedSequenceNumbers(true)),
            sourceProvider,
            handler = flowHandler)
          .withSaveOffset(1, 1.minute)

      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6|e7|e8|e9")(pid1)
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6|e7|e8|e9")(pid2)
      }

      eventually {
        latestOffsetShouldBe(allEnvelopes.last.offset)
      }
    }

    "accept events after detecting sequence number reset for flow projection" in {
      val testSettings = settings.withAcceptSequenceNumberResetAfter(10.seconds)

      val pid1 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val start = tick().instant()

      def createEnvelopesFor(
          pid: Pid,
          fromSeqNr: Int,
          toSeqNr: Int,
          fromTimestamp: Instant): immutable.IndexedSeq[EventEnvelope[String]] = {
        (fromSeqNr to toSeqNr).map { n =>
          createEnvelope(pid, n, fromTimestamp.plusSeconds(n - fromSeqNr), s"e$n")
        }
      }

      val envelopes1 = createEnvelopesFor(pid1, 1, 5, start)
      val envelopes2 = createEnvelopesFor(pid1, 1, 3, start.plusSeconds(30))
      val allEnvelopes = envelopes1 ++ envelopes2

      val (sourceProvider, sourceProbe) = createDynamicSourceProvider(allEnvelopes)

      implicit val offsetStore: DynamoDBOffsetStore =
        new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)

      val flowHandler =
        FlowWithContext[EventEnvelope[String], ProjectionContext]
          .mapAsync(1) { env =>
            repository.concatToText(env.persistenceId, env.event)
          }

      val projectionRef = spawn(
        ProjectionBehavior(
          DynamoDBProjection
            .atLeastOnceFlow(projectionId, Some(testSettings), sourceProvider, handler = flowHandler)
            .withSaveOffset(1, 1.minute)))

      val source = sourceProbe.futureValue

      envelopes1.foreach(source.sendNext)

      eventually {
        projectedValueShouldBe("e1|e2|e3|e4|e5")(pid1)
        latestOffsetShouldBe(envelopes1.last.offset)
      }

      envelopes2.foreach(source.sendNext)

      eventually {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e1|e2|e3")(pid1)
        latestOffsetShouldBe(envelopes2.last.offset)
      }

      projectionRef ! ProjectionBehavior.Stop
    }

    "replay rejected sequence numbers after detecting reset for flow projection" in {
      val testSettings = settings.withAcceptSequenceNumberResetAfter(10.seconds)

      val pid1 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val start = tick().instant()

      def createEnvelopesFor(
          pid: Pid,
          fromSeqNr: Int,
          toSeqNr: Int,
          fromTimestamp: Instant): immutable.IndexedSeq[EventEnvelope[String]] = {
        (fromSeqNr to toSeqNr).map { n =>
          createEnvelope(pid, n, fromTimestamp.plusSeconds(n - fromSeqNr), s"e$n")
        }
      }

      val envelopes1 = createEnvelopesFor(pid1, 1, 5, start)
      val envelopes2 = createEnvelopesFor(pid1, 1, 5, start.plusSeconds(30))
      val allEnvelopes = envelopes1 ++ envelopes2

      val (sourceProvider, sourceProbe) =
        createDynamicSourceProvider(allEnvelopes, enableCurrentEventsByPersistenceId = true)

      implicit val offsetStore: DynamoDBOffsetStore =
        new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)

      val flowHandler =
        FlowWithContext[EventEnvelope[String], ProjectionContext]
          .mapAsync(1) { env =>
            repository.concatToText(env.persistenceId, env.event)
          }

      val projectionRef = spawn(
        ProjectionBehavior(
          DynamoDBProjection
            .atLeastOnceFlow(projectionId, Some(testSettings), sourceProvider, handler = flowHandler)
            .withSaveOffset(1, 1.minute)))

      val source = sourceProbe.futureValue

      envelopes1.foreach(source.sendNext)

      eventually {
        projectedValueShouldBe("e1|e2|e3|e4|e5")(pid1)
        latestOffsetShouldBe(envelopes1.last.offset)
      }

      // start from seq nr 3 after reset
      envelopes2.drop(2).foreach(source.sendNext)

      eventually {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e1|e2|e3|e4|e5")(pid1)
        latestOffsetShouldBe(envelopes2.last.offset)
      }

      projectionRef ! ProjectionBehavior.Stop
    }
  }

  // FIXME see more tests in R2dbcTimestampOffsetProjectionSpec

}
