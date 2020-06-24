package akka.projection.kafka.internal

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import com.typesafe.config.Config

@InternalApi
final case class KafkaSourceProviderSettings(readOffsetDelay: FiniteDuration)

/**
 * INTERNAL API
 */
@InternalApi
object KafkaSourceProviderSettings {
  def apply(system: ActorSystem[_]): KafkaSourceProviderSettings = {
    fromConfig(system.classicSystem.settings.config.getConfig("akka.projection.kafka"))
  }

  def fromConfig(config: Config): KafkaSourceProviderSettings = {
    val readOffsetDelay = config.getDuration("read-offset-delay", MILLISECONDS).millis
    KafkaSourceProviderSettings(readOffsetDelay)
  }
}
