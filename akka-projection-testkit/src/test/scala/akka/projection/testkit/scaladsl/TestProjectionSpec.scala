/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit.scaladsl

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.projection.ProjectionId
import akka.projection.scaladsl.ActorHandler
import akka.stream.scaladsl.Source
import akka.util.Timeout
import org.scalatest.wordspec.AnyWordSpecLike

object TestProjectionSpec {
  sealed trait Command
  final case class Handle(envelope: Int, replyTo: ActorRef[Done]) extends Command

  def appender(strBuffer: StringBuffer): Behavior[Command] = Behaviors.receiveMessage {
    case Handle(i, replyTo) =>
      if (strBuffer.toString.isEmpty) strBuffer.append(i)
      else strBuffer.append("-").append(i)
      replyTo.tell(Done)
      appender(strBuffer)
  }

  class AppenderActorHandler(behavior: Behavior[Command])(implicit system: ActorSystem[_])
      extends ActorHandler[Int, Command](behavior) {
    import akka.actor.typed.scaladsl.AskPattern._

    implicit val askTimeout: Timeout = 5.seconds

    override def process(actor: ActorRef[Command], envelope: Int): Future[Done] = {
      actor.ask[Done](replyTo => Handle(envelope, replyTo))
    }
  }
}

class TestProjectionSpec extends ScalaTestWithActorTestKit with LogCapturing with AnyWordSpecLike {
  import TestProjectionSpec._

  val projectionTestKit: ProjectionTestKit = ProjectionTestKit(system)
  val projectionId: ProjectionId = ProjectionId("name", "key")

  "TestProjection" must {

    "run an ActorHandler" in {
      val strBuffer = new StringBuffer()
      val sp = TestSourceProvider(Source(1 to 6), (i: Int) => i)
      val prj = TestProjection(projectionId, sp, () => new AppenderActorHandler(appender(strBuffer)))

      // stop as soon we observe that all expected elements passed through
      projectionTestKit.run(prj) {
        strBuffer.toString shouldBe "1-2-3-4-5-6"
      }
    }
  }
}
