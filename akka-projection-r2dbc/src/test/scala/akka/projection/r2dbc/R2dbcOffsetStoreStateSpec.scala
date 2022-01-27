/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc

import java.time.Instant

import akka.projection.r2dbc.internal.R2dbcOffsetStore.Record
import akka.projection.r2dbc.internal.R2dbcOffsetStore.State
import org.scalatest.TestSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class R2dbcOffsetStoreStateSpec extends AnyWordSpec with TestSuite with Matchers {

  "R2dbcOffsetStore.State" should {
    "add records and keep track of pids and latest offset" in {
      val t0 = Instant.now()
      val state1 = State.empty
        .add(Vector(Record("p1", 1, t0), Record("p1", 2, t0.plusMillis(1)), Record("p1", 3, t0.plusMillis(2))))
      state1.byPid("p1").seqNr shouldBe 3L
      state1.latestTimestamp shouldBe t0.plusMillis(2)
      state1.latestOffset.get.seen shouldBe Map("p1" -> 3L)
      state1.oldestTimestamp shouldBe t0

      val state2 = state1.add(Vector(Record("p2", 2, t0.plusMillis(1))))
      state2.byPid("p1").seqNr shouldBe 3L
      state2.byPid("p2").seqNr shouldBe 2L
      // latest not updated because timestamp of p2 was before latest
      state2.latestTimestamp shouldBe t0.plusMillis(2)
      state2.latestOffset.get.seen shouldBe Map("p1" -> 3L)
      state2.oldestTimestamp shouldBe t0

      val state3 = state2.add(Vector(Record("p3", 10, t0.plusMillis(3))))
      state3.byPid("p1").seqNr shouldBe 3L
      state3.byPid("p2").seqNr shouldBe 2L
      state3.byPid("p3").seqNr shouldBe 10L
      state3.latestTimestamp shouldBe t0.plusMillis(3)
      state3.latestOffset.get.seen shouldBe Map("p3" -> 10L)
      state3.oldestTimestamp shouldBe t0
    }

    // reproducer of issue #173
    "include highest seqNr in seen of latestOffset" in {
      val t0 = Instant.now()
      val records =
        Vector(Record("p4", 9, t0), Record("p2", 2, t0), Record("p3", 5, t0), Record("p2", 1, t0), Record("p1", 3, t0))
      val state = State(records)
      state.byPid("p2").seqNr shouldBe 2L
      // p2 -> 2 should be included even though p2 -> 1 was added afterwards (same timestamp)
      state.latestOffset.get.seen shouldBe Map("p1" -> 3L, "p2" -> 2L, "p3" -> 5L, "p4" -> 9L)
    }

    "evict old" in {
      val t0 = Instant.now()
      val state1 = State.empty
        .add(
          Vector(
            Record("p1", 1, t0),
            Record("p2", 2, t0.plusMillis(1)),
            Record("p3", 3, t0.plusMillis(2)),
            Record("p4", 4, t0.plusMillis(3)),
            Record("p5", 5, t0.plusMillis(4))))
      state1.latestOffset.get.seen shouldBe Map("p5" -> 5L)
      state1.oldestTimestamp shouldBe t0
      state1.byPid.map { case (pid, r) => pid -> r.seqNr } shouldBe Map(
        "p1" -> 1L,
        "p2" -> 2L,
        "p3" -> 3L,
        "p4" -> 4L,
        "p5" -> 5L)

      val state2 = state1.evict(t0.plusMillis(2))
      state2.latestOffset.get.seen shouldBe Map("p5" -> 5L)
      state2.oldestTimestamp shouldBe t0.plusMillis(2)
      state2.byPid.map { case (pid, r) => pid -> r.seqNr } shouldBe Map("p3" -> 3L, "p4" -> 4L, "p5" -> 5L)
    }

    "find duplicate" in {
      val t0 = Instant.now()
      val state =
        State(Vector(Record("p1", 1, t0), Record("p2", 2, t0.plusMillis(1)), Record("p3", 3, t0.plusMillis(2))))
      state.isDuplicate(Record("p1", 1, t0)) shouldBe true
      state.isDuplicate(Record("p1", 2, t0.plusMillis(10))) shouldBe false
      state.isDuplicate(Record("p2", 1, t0)) shouldBe true
      state.isDuplicate(Record("p4", 4, t0.plusMillis(10))) shouldBe false
    }
  }
}
