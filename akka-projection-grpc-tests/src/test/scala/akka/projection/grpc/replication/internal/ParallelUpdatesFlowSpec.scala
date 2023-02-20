/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.internal

import akka.Done
import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.query.typed.EventEnvelope
import akka.projection.internal.ProjectionContextImpl
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSource
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Promise

class ParallelUpdatesFlowSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with Matchers {

  "The ParallelUpdatesFlow" should {

    "process envelopes for different persistence ids in parallel" in {
      val processProbe = createTestProbe[(String, Promise[Done])]()
      val (sourceProbe, resultF) = TestSource()
        .viaMat(new ParallelUpdatesFlow[AnyRef](4)({ envelope =>
          val done = Promise[Done]()
          processProbe ! ((envelope.persistenceId, done))
          done.future
        }))(Keep.left)
        .toMat(Sink.seq)(Keep.both)
        .run()

      sourceProbe.sendNext(element("pid1"))
      sourceProbe.sendNext(element("pid2"))
      sourceProbe.sendNext(element("pid3"))
      sourceProbe.sendNext(element("pid4"))

      val messages = processProbe.receiveMessages(4)
      sourceProbe.expectNoMessage() // no demand until previous completed
      messages.foreach(_._2.success(Done))

      sourceProbe.sendNext(element("pid5"))
      sourceProbe.sendNext(element("pid6"))
      sourceProbe.sendNext(element("pid7"))
      sourceProbe.sendNext(element("pid8"))
      val messages2 = processProbe.receiveMessages(4)
      messages2.foreach(_._2.success(Done))

      sourceProbe.sendComplete()
      resultF.futureValue should have size (8)
    }

    "make sure to never have more than one envelope in flight for the same persistence id" in {
      val processProbe = createTestProbe[(String, Promise[Done])]()
      val (sourceProbe, resultF) = TestSource()
        .viaMat(new ParallelUpdatesFlow[AnyRef](4)({ envelope =>
          val done = Promise[Done]()
          processProbe ! ((envelope.persistenceId, done))
          done.future
        }))(Keep.left)
        .toMat(Sink.seq)(Keep.both)
        .run()

      sourceProbe.sendNext(element("pid1"))
      sourceProbe.sendNext(element("pid1"))
      sourceProbe.expectNoMessage() // no demand until previous pid1 completed
      processProbe.receiveMessage()._2.success(Done)
      processProbe.receiveMessage()._2.success(Done)

      sourceProbe.sendNext(element("pid3"))
      sourceProbe.sendNext(element("pid4"))

      processProbe.receiveMessages(2).foreach(_._2.success(Done))

      sourceProbe.sendComplete()
      resultF.futureValue should have size (4)
    }

    "handle immediate complete" in {
      Source.empty
        .via(new ParallelUpdatesFlow[AnyRef](4)({ _ => throw TestException("boom") }))
        .runWith(Sink.ignore)
        .futureValue
    }

    "fail if future throws" in {
      Source
        .single(element("pid1"))
        .via(new ParallelUpdatesFlow[AnyRef](4)({ _ => throw TestException("boom") }))
        .runWith(Sink.ignore)
        .failed
        .futureValue
    }
  }

  private def element(pid: String) =
    (EventEnvelope[AnyRef](null, pid, -1L, null, -1L, "", 0), ProjectionContextImpl(null, null, null, -1))
}
