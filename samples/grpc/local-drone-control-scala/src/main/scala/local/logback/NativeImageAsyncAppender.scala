package local.logback

import ch.qos.logback.classic.AsyncAppender
import ch.qos.logback.classic.spi.ILoggingEvent

/**
 * Lazy version of AsyncAppender for native images, as threads can't be started during native image build.
 */
class NativeImageAsyncAppender extends AsyncAppender {

  @volatile private[this] var active: Boolean = false

  override def start(): Unit = {
    if (!isNativeImageBuild) {
      super.start()
      active = true
    }
  }

  override def doAppend(eventObject: ILoggingEvent): Unit = {
    if (!isNativeImageBuild) {
      if (!active) startNow()
      super.doAppend(eventObject)
    }
  }

  private def startNow(): Unit = synchronized {
    if (!active) start()
  }

  // method, so that it's not fixed at build time
  private def isNativeImageBuild = sys.props.get("org.graalvm.nativeimage.imagecode").contains("buildtime")
}
