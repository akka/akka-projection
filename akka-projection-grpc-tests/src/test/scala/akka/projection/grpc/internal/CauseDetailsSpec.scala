/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.internal

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CauseDetailsSpec extends AnyWordSpec with Matchers {

  "CauseDetails" must {

    "render the chain with class names" in {
      val ex = new RuntimeException("outer", new IllegalStateException("middle", new IllegalArgumentException("root")))
      CauseDetails(ex) shouldBe
      "RuntimeException: outer, caused by: IllegalStateException: middle, caused by: IllegalArgumentException: root"
    }

    "keep only the class name when the wrapper message already contains the message" in {
      val ex = new RuntimeException("wrapped: boom", new IllegalStateException("boom"))
      CauseDetails(ex) shouldBe "RuntimeException: wrapped: boom, caused by: IllegalStateException"
    }

    "keep the root cause when the chain is cut" in {
      val root = new IllegalArgumentException("the root reason")
      val chain = (1 to 10).foldLeft[Throwable](root)((cause, n) => new RuntimeException(s"level $n", cause))
      val details = CauseDetails(chain)
      details should endWith("IllegalArgumentException: the root reason")
      details should include("level 10")
      (details should not).include("level 5")
    }

    "only render the first line and bound the length" in {
      val details = CauseDetails(new RuntimeException("first\nsecond"))
      details shouldBe "RuntimeException: first"
      CauseDetails(new RuntimeException("x" * 1000)).length should be < 300
    }

    "replace control and line separator characters" in {
      val details = CauseDetails(new RuntimeException("a\u001b[31mb c\u0085d\te"))
      details shouldBe "RuntimeException: a?[31mb?c?d?e"
    }

    "handle cause chains that are cyclic" in {
      lazy val a: Throwable = new RuntimeException("a") { override def getCause: Throwable = b }
      lazy val b: Throwable = new RuntimeException("b") { override def getCause: Throwable = a }
      CauseDetails(a) should include("caused by")
    }
  }
}
