package akka.projection

import java.time

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.MILLISECONDS
import scala.concurrent.duration._

import akka.actor.ClassicActorSystemProvider
import akka.util.JavaDurationConverters._
import com.typesafe.config.Config

trait AtLeastOnceSettings {
  def saveOffsetAfterEnvelopes: Int
  def saveOffsetAfterDuration: FiniteDuration

  /**
   * Java API
   */
  def getSaveOffsetAfterDuration(): java.time.Duration

  def withSaveOffsetAfterEnvelopes(afterElements: Int): AtLeastOnceSettings

  /**
   * Scala API
   */
  def withSaveOffsetAfterDuration(afterDuration: FiniteDuration): AtLeastOnceSettings

  /**
   * Java API
   */
  def withSaveOffsetAfterDuration(afterDuration: java.time.Duration): AtLeastOnceSettings
}

object AtLeastOnceSettings {

  /**
   * Java API
   */
  def create(system: ClassicActorSystemProvider): AtLeastOnceSettings = apply(system)

  def apply(system: ClassicActorSystemProvider): AtLeastOnceSettings = {
    fromConfig(system.classicSystem.settings.config.getConfig("akka.projection.at-least-once"))
  }

  def fromConfig(config: Config) =
    new AtLeastOnceSettingsImpl(
      config.getInt("save-offset-after-envelopes"),
      config.getDuration("save-offset-after-duration", MILLISECONDS).millis)

}

private[akka] class AtLeastOnceSettingsImpl(
    val saveOffsetAfterEnvelopes: Int,
    val saveOffsetAfterDuration: FiniteDuration)
    extends AtLeastOnceSettings {

  /**
   * Java API
   */
  override def getSaveOffsetAfterDuration(): time.Duration = saveOffsetAfterDuration.asJava

  override def withSaveOffsetAfterEnvelopes(afterEnvelopes: Int): AtLeastOnceSettings =
    copy(saveOffsetAfterEnvelopes = afterEnvelopes)

  /**
   * Scala API
   */
  override def withSaveOffsetAfterDuration(afterDuration: FiniteDuration): AtLeastOnceSettings =
    copy(saveOffsetAfterDuration = afterDuration)

  /**
   * Java API
   */
  override def withSaveOffsetAfterDuration(afterDuration: java.time.Duration): AtLeastOnceSettings =
    copy(saveOffsetAfterDuration = afterDuration.asScala)

  private[akka] def copy(
      saveOffsetAfterEnvelopes: Int = saveOffsetAfterEnvelopes,
      saveOffsetAfterDuration: FiniteDuration = saveOffsetAfterDuration): AtLeastOnceSettings =
    new AtLeastOnceSettingsImpl(saveOffsetAfterEnvelopes, saveOffsetAfterDuration)
}
