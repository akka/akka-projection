/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.pattern.StatusReply
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
object EventWriterSpec {
  def config =
    ConfigFactory.parseString(
      // until we have it in reference.conf
      """
      akka.persistence.typed.event-writer {
        max-batch-size = 10
        ask-timeout = 5s
      }
      """)
}

class EventWriterSpec extends ScalaTestWithActorTestKit(EventWriterSpec.config) with AnyWordSpecLike with LogCapturing {

  implicit val ec: ExecutionContext = testKit.system.executionContext

  "The event writer" should {

    "handle duplicates" in {
      val writer = spawn(EventWriter("akka.persistence.journal.inmem"))

      val probe = createTestProbe[StatusReply[EventWriter.WriteAck]]()
      writer ! EventWriter.Write("pid1", 1L, "one", None, Set.empty, probe.ref)
      probe.receiveMessage().getValue

      // should also be ack:ed
      writer ! EventWriter.Write("pid1", 1L, "one", None, Set.empty, probe.ref)
      probe.receiveMessage().getValue
    }

    "handle batched duplicates" in {
      // FIXME we can't really know inmem isn't fast enough to insert each one by one, over on the Akka side we
      //       have SteppingInMemJournal that we could perhaps use for deterministic tests, or add a delay config
      //       to the inmem to easier simulate actual db
      val writer = spawn(EventWriter("akka.persistence.journal.inmem"))
      val probe = createTestProbe[StatusReply[EventWriter.WriteAck]]()
      for (n <- 0 to 10) {
        writer ! EventWriter.Write("pid1", n.toLong, n.toString, None, Set.empty, probe.ref)
      }
      for (n <- 0 to 10) {
        writer ! EventWriter.Write("pid1", n.toLong, n.toString, None, Set.empty, probe.ref)
      }
      probe.receiveMessages(20) // all should be ack:ed
    }

    "handle batches with half duplicates" in {
      val writer = spawn(EventWriter("akka.persistence.journal.inmem"))
      val probe = createTestProbe[StatusReply[EventWriter.WriteAck]]()
      for (n <- 0 to 10) {
        writer ! EventWriter.Write("pid1", n.toLong, n.toString, None, Set.empty, probe.ref)
      }
      for (n <- 5 to 15) {
        writer ! EventWriter.Write("pid1", n.toLong, n.toString, None, Set.empty, probe.ref)
      }
      probe.receiveMessages(20) // all should be ack:ed
    }

    "handle writes to many pids" in {
      val writer = spawn(EventWriter("akka.persistence.journal.inmem"))
      val probe = createTestProbe[StatusReply[EventWriter.WriteAck]]()
      (0 to 1000).map { pidN =>
        Future {
          for (n <- 0 to 20) {
            writer ! EventWriter.Write(s"pid$pidN", n.toLong, n.toString, None, Set.empty, probe.ref)
          }
          Done
        }
      }
      probe.receiveMessages(20 * 1000, 20.seconds)
    }
  }

}
