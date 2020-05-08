/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cluster.scaladsl

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise

import akka.Done
import akka.NotUsed
import akka.actor.ClassicActorSystemProvider
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.stream.KillSwitches
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

object ClusterProjectionRunnerSpec {
  def config: Config = ConfigFactory.parseString("""
      akka {
        loglevel = "DEBUG"
        actor.provider = cluster
   
        remote.artery {
          canonical {
            hostname = "127.0.0.1"
            port = 0
          }
        }
      }
      """)
}
class ClusterProjectionRunnerSpec
    extends ScalaTestWithActorTestKit(ClusterProjectionRunnerSpec.config)
    with AnyWordSpecLike
    with LogCapturing {

  // single node cluster => joining itself
  private val cluster = Cluster(system)

  "A ClusterRunner" must {

    "distribute projections using ShardedDaemonProcess" in {

      cluster.manager ! Join(cluster.selfMember.address)

      val name = "distributed-projection"
      val probesAndIds = (1 to 2).map { i =>
        (testKit.createTestProbe[ProbeMessage](s"test-probe-$i"), ProjectionId(name, i.toString))
      }

      val projections =
        probesAndIds.map {
          case (probe, projectionId) => () => TestProjection(Source(1 to 5), projectionId, probe)
        }

      ClusterProjectionRunner.init(system, name, projections)

      val (probe1, projectionId1) = probesAndIds(0)
      val (probe2, projectionId2) = probesAndIds(1)

      probe1.expectMessage(StartObserved(projectionId1))
      probe1.expectMessage(Consumed(1, "1"))
      probe1.expectMessage(Consumed(2, "1-2"))

      probe2.expectMessage(StartObserved(projectionId2))
      probe2.expectMessage(Consumed(1, "1"))
      probe2.expectMessage(Consumed(2, "1-2"))

    }
  }

  sealed trait ProbeMessage
  case class StartObserved(projectionId: ProjectionId) extends ProbeMessage
  case class Consumed(n: Int, currentState: String) extends ProbeMessage
  case class StopObserved(projectionId: ProjectionId) extends ProbeMessage

  /*
   * This TestProjection has a internal state that we can use to prove that on restart,
   * the actor is taking a new projection instance.
   */
  case class TestProjection(src: Source[Int, NotUsed], projectionId: ProjectionId, testProbe: TestProbe[ProbeMessage])
      extends Projection[Int] {

    private val strBuffer = new StringBuffer("")

    private val killSwitch = KillSwitches.shared(projectionId.id)
    private val promiseToStop = Promise[Done]

    override def run()(implicit systemProvider: ClassicActorSystemProvider): Unit = {
      val done = mappedSource.runWith(Sink.ignore)
      testProbe.ref ! StartObserved(projectionId)
      promiseToStop.completeWith(done)
    }

    private def process(i: Int): Future[Done] = {
      concat(i)
      testProbe.ref ! Consumed(i, strBuffer.toString)
      Future.successful(Done)
    }

    private[projection] def mappedSource()(implicit systemProvider: ClassicActorSystemProvider): Source[Done, _] = {
      src.via(killSwitch.flow).mapAsync(1)(i => process(i))
    }

    private def concat(i: Int) = {
      if (strBuffer.toString.isEmpty) strBuffer.append(i)
      else strBuffer.append("-").append(i)
    }

    override def stop()(implicit ec: ExecutionContext): Future[Done] = {
      killSwitch.shutdown()
      val stopFut = promiseToStop.future
      stopFut.foreach(_ => testProbe.ref ! StopObserved(projectionId))
      stopFut
    }
  }

}
