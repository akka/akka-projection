/*
 * Copyright (C) 2022-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb

import java.lang
import java.util.UUID
import java.util.concurrent.CompletionException
import java.util.concurrent.ConcurrentHashMap
import java.util.{ HashMap => JHashMap }

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.Failure
import scala.util.Success

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.dynamodb.query.scaladsl.DynamoDBReadJournal
import akka.persistence.query.Offset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.RetentionCriteria
import akka.projection.Projection
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionContext
import akka.projection.ProjectionId
import akka.projection.dynamodb.scaladsl.DynamoDBProjection
import akka.projection.dynamodb.scaladsl.DynamoDBTransactHandler
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.FlowWithContext
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
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

object EventSourcedEndToEndSpec {

  private val log = LoggerFactory.getLogger(classOf[EventSourcedEndToEndSpec])

  val config: Config = ConfigFactory
    .parseString("""
    akka.persistence.dynamodb {
      query {
        refresh-interval = 500 millis
        # stress more by using a small buffer
        buffer-size = 10

        backtracking.behind-current-time = 5 seconds
      }
    }
    """)
    .withFallback(TestConfig.config)

  object Persister {
    sealed trait Command
    final case class Persist(payload: Any) extends Command
    final case class PersistWithAck(payload: Any, replyTo: ActorRef[Done]) extends Command
    final case class PersistAll(payloads: List[Any]) extends Command
    final case class Ping(replyTo: ActorRef[Done]) extends Command
    final case class Stop(replyTo: ActorRef[Done]) extends Command

    def apply(pid: PersistenceId): Behavior[Command] = {
      Behaviors.setup { context =>
        EventSourcedBehavior[Command, Any, String](persistenceId = pid, "", {
          (_, command) =>
            command match {
              case command: Persist =>
                context.log.debug(
                  "Persist [{}], pid [{}], seqNr [{}]",
                  command.payload,
                  pid.id,
                  EventSourcedBehavior.lastSequenceNumber(context) + 1)
                Effect.persist(command.payload)
              case command: PersistWithAck =>
                context.log.debug(
                  "Persist [{}], pid [{}], seqNr [{}]",
                  command.payload,
                  pid.id,
                  EventSourcedBehavior.lastSequenceNumber(context) + 1)
                Effect.persist(command.payload).thenRun(_ => command.replyTo ! Done)
              case command: PersistAll =>
                if (context.log.isDebugEnabled)
                  context.log.debug(
                    "PersistAll [{}], pid [{}], seqNr [{}]",
                    command.payloads.mkString(","),
                    pid.id,
                    EventSourcedBehavior.lastSequenceNumber(context) + 1)
                Effect.persist(command.payloads)
              case Ping(replyTo) =>
                replyTo ! Done
                Effect.none
              case Stop(replyTo) =>
                replyTo ! Done
                Effect.stop()
            }
        }, (_, _) => "")
          .withRetention(RetentionCriteria.snapshotEvery(5))
      }
    }
  }

  final case class Processed(projectionId: ProjectionId, envelope: EventEnvelope[String])

  final case class StartParams(
      entityType: String,
      projectionName: String,
      nrOfProjections: Int,
      processedProbe: TestProbe[Processed])

  object TestTable {
    val name = "endtoend_spec"

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

    def findById(id: String)(client: DynamoDbAsyncClient, system: ActorSystem[_]): Future[Option[String]] = {
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
            Some(response.item.get(Attributes.Payload).s())
          else None
        }
        .recoverWith {
          case c: CompletionException =>
            Future.failed(c.getCause)
        }(ExecutionContext.parasitic)
    }
  }

  class TestHandler(
      projectionId: ProjectionId,
      probe: ActorRef[Processed],
      processedEvents: ConcurrentHashMap[String, java.lang.Boolean])
      extends Handler[EventEnvelope[String]] {

    override def process(envelope: EventEnvelope[String]): Future[Done] = {
      log.debug(
        "{} Processed {} [{}], pid [{}], seqNr [{}]",
        projectionId.key,
        if (processedEvents.containsKey(envelope.event)) "duplicate event" else "event",
        envelope.event,
        envelope.persistenceId,
        envelope.sequenceNr)
      // could be at-least once
      if (processedEvents.putIfAbsent(envelope.event, true) == null)
        probe ! Processed(projectionId, envelope)
      Future.successful(Done)
    }
  }

  class GroupedTestHandler(
      projectionId: ProjectionId,
      probe: ActorRef[Processed],
      processedEvents: ConcurrentHashMap[String, java.lang.Boolean])
      extends Handler[Seq[EventEnvelope[String]]] {
    val delegate = new TestHandler(projectionId, probe, processedEvents)

    override def process(envelopes: Seq[EventEnvelope[String]]): Future[Done] = {
      envelopes.foreach(delegate.process)
      Future.successful(Done)
    }
  }

  private def flowTestHandler(
      projectionId: ProjectionId,
      probe: ActorRef[Processed],
      processedEvents: ConcurrentHashMap[String, java.lang.Boolean]) = {

    FlowWithContext[EventEnvelope[String], ProjectionContext].map { envelope =>
      log.debug(
        "{} Processed {} [{}], pid [{}], seqNr [{}]",
        projectionId.key,
        if (processedEvents.containsKey(envelope.event)) "duplicate event" else "event",
        envelope.event,
        envelope.persistenceId,
        envelope.sequenceNr)
      // could be at-least once
      if (processedEvents.putIfAbsent(envelope.event, true) == null)
        probe ! Processed(projectionId, envelope)
      Done
    }
  }

  class TransactTestHandler(
      projectionId: ProjectionId,
      probe: ActorRef[Processed],
      client: DynamoDbAsyncClient,
      system: ActorSystem[_])
      extends DynamoDBTransactHandler[EventEnvelope[String]] {
    import system.executionContext

    override def process(envelope: EventEnvelope[String]): Future[Iterable[TransactWriteItem]] = {
      log.debug(
        "{} Processed event [{}], pid [{}], seqNr [{}]",
        projectionId.key,
        envelope.event,
        envelope.persistenceId,
        envelope.sequenceNr)

      val id = s"${envelope.persistenceId}-${envelope.sequenceNr}"
      TestTable.findById(id)(client, system).map {
        case None =>
          val attributes = new JHashMap[String, AttributeValue]
          attributes.put(TestTable.Attributes.Id, AttributeValue.fromS(id))
          attributes.put(TestTable.Attributes.Payload, AttributeValue.fromS(envelope.event))
          probe ! Processed(projectionId, envelope)
          Seq(TransactWriteItem.builder().put(Put.builder().tableName(TestTable.name).item(attributes).build()).build())
        case Some(_) =>
          log.error(
            "{} Processed duplicate event [{}], pid [{}], seqNr [{}]",
            projectionId.key,
            envelope.event,
            envelope.persistenceId,
            envelope.sequenceNr)
          // this will be detected as duplicate and fail the test
          probe ! Processed(projectionId, envelope)
          Nil
      }
    }
  }

  class GroupedTransactTestHandler(
      projectionId: ProjectionId,
      probe: ActorRef[Processed],
      client: DynamoDbAsyncClient,
      system: ActorSystem[_])
      extends DynamoDBTransactHandler[Seq[EventEnvelope[String]]] {
    import system.executionContext
    val delegate = new TransactTestHandler(projectionId, probe, client, system)

    override def process(envelopes: Seq[EventEnvelope[String]]): Future[Iterable[TransactWriteItem]] = {
      envelopes.foldLeft(Future.successful(Vector.empty[TransactWriteItem])) {
        case (acc, env) =>
          acc.flatMap { accWriteItems =>
            delegate.process(env).map(accWriteItems ++ _)
          }
      }
    }
  }

}

class EventSourcedEndToEndSpec
    extends ScalaTestWithActorTestKit(EventSourcedEndToEndSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {
  import EventSourcedEndToEndSpec._

  override def typedSystem: ActorSystem[_] = system

  override protected def beforeAll(): Unit = {
    if (localDynamoDB)
      Await.result(TestTable.create(client, system), 10.seconds)
    super.beforeAll()
  }

  private var processedEventsPerProjection: Map[ProjectionId, ConcurrentHashMap[String, java.lang.Boolean]] = Map.empty

  private def processedEvents(projectionId: ProjectionId): ConcurrentHashMap[String, lang.Boolean] = {
    processedEventsPerProjection.get(projectionId) match {
      case None =>
        val processedEvents = new ConcurrentHashMap[String, java.lang.Boolean]
        processedEventsPerProjection = processedEventsPerProjection.updated(projectionId, processedEvents)
        processedEvents
      case Some(processedEvents) =>
        processedEvents
    }
  }

  private def startAtLeastOnceProjections(startParams: StartParams): Vector[ActorRef[ProjectionBehavior.Command]] = {
    startProjections(
      startParams,
      (projectionId, sourceProvider) =>
        DynamoDBProjection
          .atLeastOnce(
            projectionId,
            Some(settings),
            sourceProvider = sourceProvider,
            handler =
              () => new TestHandler(projectionId, startParams.processedProbe.ref, processedEvents(projectionId))))
  }

  private def startAtLeastOnceGroupedProjections(
      startParams: StartParams): Vector[ActorRef[ProjectionBehavior.Command]] = {
    startProjections(
      startParams,
      (projectionId, sourceProvider) =>
        DynamoDBProjection
          .atLeastOnceGroupedWithin(
            projectionId,
            Some(settings),
            sourceProvider = sourceProvider,
            handler =
              () => new GroupedTestHandler(projectionId, startParams.processedProbe.ref, processedEvents(projectionId)))
          .withGroup(3, groupAfterDuration = 200.millis))
  }

  private def startAtLeastOnceFlowProjections(
      startParams: StartParams): Vector[ActorRef[ProjectionBehavior.Command]] = {
    startProjections(
      startParams,
      (projectionId, sourceProvider) =>
        DynamoDBProjection
          .atLeastOnceFlow(
            projectionId,
            Some(settings),
            sourceProvider = sourceProvider,
            flowTestHandler(projectionId, startParams.processedProbe.ref, processedEvents(projectionId))))
  }

  private def startExactlyOnceProjections(startParams: StartParams): Vector[ActorRef[ProjectionBehavior.Command]] = {
    startProjections(
      startParams,
      (projectionId, sourceProvider) =>
        DynamoDBProjection
          .exactlyOnce(
            projectionId,
            Some(settings),
            sourceProvider = sourceProvider,
            handler = () => new TransactTestHandler(projectionId, startParams.processedProbe.ref, client, system)))
  }

  private def startExactlyOnceGroupedProjections(
      startParams: StartParams): Vector[ActorRef[ProjectionBehavior.Command]] = {
    startProjections(
      startParams,
      (projectionId, sourceProvider) =>
        DynamoDBProjection
          .exactlyOnceGroupedWithin(
            projectionId,
            Some(settings),
            sourceProvider = sourceProvider,
            handler =
              () => new GroupedTransactTestHandler(projectionId, startParams.processedProbe.ref, client, system))
          .withGroup(3, groupAfterDuration = 200.millis))
  }

  private def startProjections(
      startParams: StartParams,
      projectionFactory: (
          ProjectionId,
          SourceProvider[Offset, EventEnvelope[String]]) => Projection[EventEnvelope[String]])
      : Vector[ActorRef[ProjectionBehavior.Command]] = {
    import startParams._
    val sliceRanges = EventSourcedProvider.sliceRanges(system, DynamoDBReadJournal.Identifier, nrOfProjections)

    sliceRanges.map { range =>
      val projectionId = ProjectionId(projectionName, s"${range.min}-${range.max}")

      val sourceProvider =
        EventSourcedProvider
          .eventsBySlices[String](system, DynamoDBReadJournal.Identifier, entityType, range.min, range.max)

      val projection = projectionFactory(projectionId, sourceProvider)
      spawn(ProjectionBehavior(projection))
    }.toVector
  }

  private def mkEvent(n: Int): String = f"e$n%05d"

  private def assertEventsProcessed(
      expectedEvents: Vector[String],
      processedProbe: TestProbe[Processed],
      verifyProjectionId: Boolean): Unit = {
    val expectedNumberOfEvents = expectedEvents.size
    var processed = Vector.empty[Processed]

    (1 to expectedNumberOfEvents).foreach { _ =>
      // not using receiveMessages(expectedEvents) for better logging in case of failure
      try {
        processed :+= processedProbe.receiveMessage(15.seconds)
      } catch {
        case e: AssertionError =>
          val missing = expectedEvents.diff(processed.map(_.envelope.event))
          log.error(s"Processed [${processed.size}] events, but expected [$expectedNumberOfEvents]. " +
          s"Missing [${missing.mkString(",")}]. " +
          s"Received [${processed.map(p => s"(${p.envelope.event}, ${p.envelope.persistenceId}, ${p.envelope.sequenceNr})").mkString(", ")}]. ")
          throw e
      }
    }

    if (verifyProjectionId) {
      val byPid = processed.groupBy(_.envelope.persistenceId)
      byPid.foreach {
        case (pid, processedByPid) =>
          withClue(s"PersistenceId [$pid]: ") {
            // all events of a pid must be processed by the same projection instance
            processedByPid.map(_.projectionId).toSet.size shouldBe 1
            // processed events in right order
            processedByPid.map(_.envelope.sequenceNr) shouldBe (1 to processedByPid.size).toVector
          }
      }
    }
  }

  private def test(
      startParams: StartParams,
      startProjectionsFactory: () => Vector[ActorRef[ProjectionBehavior.Command]]): Unit = {
    val numberOfEntities = 20 // increase this for longer testing
    val numberOfEvents = numberOfEntities * 10
    import startParams.entityType
    processedEventsPerProjection = Map.empty

    val entities = (0 until numberOfEntities).map { n =>
      val persistenceId = PersistenceId(entityType, s"p$n")
      spawn(Persister(persistenceId), s"$entityType-p$n")
    }

    // write some before starting the projections
    var n = 1
    while (n <= numberOfEvents / 4) {
      val p = n % numberOfEntities
      // mix some persist 1 and persist 3 events
      if (n % 7 == 0) {
        entities(p) ! Persister.PersistAll((0 until 3).map(i => mkEvent(n + i)).toList)
        n += 3
      } else {
        entities(p) ! Persister.Persist(mkEvent(n))
        n += 1
      }

      if (n % 10 == 0)
        Thread.sleep(50)
      else if (n % 25 == 0)
        Thread.sleep(1500)
    }

    val projections = startProjectionsFactory()

    // give them some time to start before writing more events
    Thread.sleep(200)

    while (n <= numberOfEvents) {
      val p = n % numberOfEntities
      entities(p) ! Persister.Persist(mkEvent(n))

      // stop projections
      if (n == numberOfEvents / 2) {
        projections.foreach { ref =>
          ref ! ProjectionBehavior.Stop
        }
      }

      // wait until stopped
      if (n == (numberOfEvents / 2) + 10) {
        val probe = createTestProbe()
        projections.foreach { ref =>
          probe.expectTerminated(ref)
        }
      }

      // resume projections again
      if (n == (numberOfEvents / 2) + 20)
        startProjectionsFactory()

      if (n % 10 == 0)
        Thread.sleep(50)
      else if (n % 25 == 0)
        Thread.sleep(1500)

      n += 1
    }

    val expectedEvents = (1 to numberOfEvents).map(mkEvent).toVector
    assertEventsProcessed(expectedEvents, startParams.processedProbe, verifyProjectionId = true)

    projections.foreach(_ ! ProjectionBehavior.Stop)
    val probe = createTestProbe()
    projections.foreach { ref =>
      probe.expectTerminated(ref)
    }
  }

  private def newStartParams(): StartParams = {
    val entityType = nextEntityType()
    val projectionName = UUID.randomUUID().toString
    val processedProbe = createTestProbe[Processed]()
    StartParams(entityType, projectionName, nrOfProjections = 4, processedProbe)
  }

  s"A DynamoDB projection with eventsBySlices source" must {

    "handle all events atLeastOnce" in {
      val startParams = newStartParams()
      test(startParams, () => startAtLeastOnceProjections(startParams))
    }

    "handle all events atLeastOnceGrouped" in {
      val startParams = newStartParams()
      test(startParams, () => startAtLeastOnceGroupedProjections(startParams))
    }

    "handle all events atLeastOnceFlow" in {
      val startParams = newStartParams()
      test(startParams, () => startAtLeastOnceFlowProjections(startParams))
    }

    "handle all events exactlyOnce" in {
      val startParams = newStartParams()
      test(startParams, () => startExactlyOnceProjections(startParams))
    }

    "handle all events exactlyOnceGrouped" in {
      val startParams = newStartParams()
      test(startParams, () => startExactlyOnceGroupedProjections(startParams))
    }

  }
}
