/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit

import akka.{ Done, NotUsed }
import akka.actor.ClassicActorSystemProvider
import akka.actor.testkit.typed.scaladsl.{ ScalaTestWithActorTestKit, TestProbe }
import akka.projection.Projection
import akka.stream.{ KillSwitch, KillSwitches }
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.{ Keep, Sink }
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import org.scalatest.wordspec.AnyWordSpecLike
import akka.actor.typed.scaladsl.adapter._

import scala.concurrent.{ Future, Promise }

class ProjectionTestKitSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike {

  val projectionTestKit: ProjectionTestKit = ProjectionTestKit(testKit)

  "ProjectionTestKit" must {

    "run a projection" in {
      val probe = testKit.createTestProbe[Int]("projection-probe")
      val prj = TestProjection(Source(1 to 5), probe)

      val stopped =
        projectionTestKit.run(prj) {
          probe.expectMessage(1)
          probe.expectMessage(2)
          probe.expectMessage(3)
          probe.expectMessage(4)
          probe.expectMessage(5)
        }
    }

  }

  case class TestProjection(src: Source[Int, NotUsed], probe: TestProbe[Int]) extends Projection[Int] {

    override def name: String = "test-projection"
    override def key: String = "00"

    private var shutdown: Option[KillSwitch] = None
    private val promiseToStop = Promise[Done]

    override def processElement(elt: Int): Future[Int] = {
      probe.ref ! elt
      Future.successful(elt)
    }
    override def start()(implicit systemProvider: ClassicActorSystemProvider): Unit = {

      implicit val ec = systemProvider.classicSystem.dispatcher

      val (killSwitch, sinkMat) =
        src
          .viaMat(KillSwitches.single)(Keep.right)
          .mapAsync(1) { elt => processElement(elt).map(_ => Done) }
          .toMat(Sink.ignore)(Keep.both)
          .run()

      shutdown = Some(killSwitch)
      promiseToStop.completeWith(sinkMat)
    }

    override def stop(): Future[Done] = {
      shutdown.foreach(_.shutdown())
      promiseToStop.future
    }
  }
}
