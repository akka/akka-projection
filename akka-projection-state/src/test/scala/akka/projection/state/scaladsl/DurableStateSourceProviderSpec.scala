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
import akka.stream.scaladsl.Sink

object DurableStateSourceProviderSpec {
  val InMemPluginId = "akka.persistence.query.state.inmem"
  def conf: Config = ConfigFactory.parseString(s"""
    akka.loglevel = INFO
    akka.persistence.state.plugin = $InMemPluginId
    akka.persistence.query.state.inmem {
      class = "akka.persistence.query.state.inmem.InmemDurableStateStoreProvider"
    }
    """)
  final case class Record(id: Int, name: String)
}

class DurableStateSourceProviderSpec
    extends ScalaTestWithActorTestKit(DurableStateSourceProviderSpec.conf)
    with AnyWordSpecLike {
  "A DurableStateSourceProvider" must {
    import DurableStateSourceProviderSpec._
    "provide a changes by tag SourceProvider" in {
      val record = Record(0, "Name-1")
      val tag = "tag-a"

      val durableStateStore: DurableStateUpdateStore[Record] =
        DurableStateStoreRegistry(system)
          .durableStateStoreFor[DurableStateUpdateStore[Record]](InMemPluginId)

      whenReady(durableStateStore.upsertObject("persistent-id", 0L, record, tag)) { _ =>
        val sourceProvider =
          DurableStateSourceProvider.changesByTag[Record](system, InMemPluginId, tag)
        // This test depends on source completing, which is not correctly implemented on InMemDurableStateStore at this moment
        // When does currentChanges get used?
        whenReady(sourceProvider.source(() => Future.successful[Option[Offset]](None))) { source =>
          whenReady(source.runWith(Sink.seq[DurableStateChange[Record]])) { res =>
            res.size should equal(1)
            res(0).value should be(record)
            res(0).revision should be(0L)
          }
        }
      }
    }
  }

}
