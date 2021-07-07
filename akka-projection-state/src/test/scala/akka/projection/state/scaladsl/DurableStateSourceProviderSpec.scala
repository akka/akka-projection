package akka.projection.state.scaladsl

import scala.concurrent.Future
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl._
import akka.persistence.query.DurableStateChange
import akka.persistence.query.Offset
import akka.persistence.state.DurableStateStoreRegistry
import akka.persistence.state.scaladsl._
import akka.stream.testkit.scaladsl.TestSink

object DurableStateSourceProviderSpec {
  val InMemPluginId = "akka.persistence.testkit.state"
  def conf: Config = ConfigFactory.parseString(s"""
    akka.loglevel = INFO
    akka.persistence.state.plugin = $InMemPluginId
    akka.persistence.testkit.state {
      class = "akka.persistence.testkit.state.PersistenceTestKitDurableStateStoreProvider"
    }
    """)
  final case class Record(id: Int, name: String)
}

class DurableStateSourceProviderSpec
    extends ScalaTestWithActorTestKit(DurableStateSourceProviderSpec.conf)
    with AnyWordSpecLike {
  "A DurableStateSourceProvider" must {
    import DurableStateSourceProviderSpec._

    "provide changes by tag" in {
      val record = Record(0, "Name-1")
      val tag = "tag-a"
      val recordChange = Record(0, "Name-2")

      val durableStateStore: DurableStateUpdateStore[Record] =
        DurableStateStoreRegistry(system)
          .durableStateStoreFor[DurableStateUpdateStore[Record]](InMemPluginId)
      implicit val ec = system.classicSystem.dispatcher
      val fut = Future.sequence(
        Vector(
          durableStateStore.upsertObject("persistent-id-1", 0L, record, tag),
          durableStateStore.upsertObject("persistent-id-2", 0L, record, "tag-b"),
          durableStateStore.upsertObject("persistent-id-1", 1L, recordChange, tag),
          durableStateStore.upsertObject("persistent-id-3", 0L, record, "tag-c")))
      whenReady(fut) { _ =>
        val sourceProvider =
          DurableStateSourceProvider.changesByTag[Record](system, InMemPluginId, tag)

        whenReady(sourceProvider.source(() => Future.successful[Option[Offset]](None))) { source =>
          val stateChange = source
            .runWith(TestSink[DurableStateChange[Record]]())
            .request(1)
            .expectNext()

          stateChange.value should be(recordChange)
          stateChange.revision should be(1L)
        }
      }
    }
  }

}
