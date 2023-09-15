package local.logback;

import ch.qos.logback.classic.AsyncAppender;
import ch.qos.logback.classic.spi.ILoggingEvent;

/** Lazy version of AsyncAppender, as threads can't be started during native image build. */
public class NativeImageAsyncAppender extends AsyncAppender {

  private volatile boolean active = false;

  @Override
  public void start() {
    if (isNotNativeImageBuild()) {
      super.start();
      active = true;
    }
  }

  @Override
  public void doAppend(ILoggingEvent eventObject) {
    if (isNotNativeImageBuild()) {
      if (!active) startNow();
      super.doAppend(eventObject);
    }
  }

  private synchronized void startNow() {
    if (!active) start();
  }

  // method, so that it's not fixed at build time
  private boolean isNotNativeImageBuild() {
    return !System.getProperty("org.graalvm.nativeimage.imagecode", "").contains("buildtime");
  }
}
