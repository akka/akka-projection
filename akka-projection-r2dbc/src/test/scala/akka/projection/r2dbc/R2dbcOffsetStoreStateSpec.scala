/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc

import java.time.Instant

import akka.projection.r2dbc.internal.R2dbcOffsetStore.Pid
import akka.projection.r2dbc.internal.R2dbcOffsetStore.Record
import akka.projection.r2dbc.internal.R2dbcOffsetStore.SeqNr
import akka.projection.r2dbc.internal.R2dbcOffsetStore.State
import org.scalatest.TestSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class R2dbcOffsetStoreStateSpec extends AnyWordSpec with TestSuite with Matchers {

  def createRecord(pid: Pid, seqNr: SeqNr, timestamp: Instant): Record = {
    val slice = math.abs(pid.hashCode % 1024)
    Record(slice, pid, seqNr, timestamp)
  }

  "R2dbcOffsetStore.State" should {
    "add records and keep track of pids and latest offset" in {
      val t0 = TestClock.nowMillis().instant()
      val state1 = State.empty
        .add(
          Vector(
            createRecord("p1", 1, t0),
            createRecord("p1", 2, t0.plusMillis(1)),
            createRecord("p1", 3, t0.plusMillis(2))))
      state1.byPid("p1").seqNr shouldBe 3L
      state1.latestTimestamp shouldBe t0.plusMillis(2)
      state1.latestOffset.get.seen shouldBe Map("p1" -> 3L)
      state1.oldestTimestamp shouldBe t0

      val state2 = state1.add(Vector(createRecord("p2", 2, t0.plusMillis(1))))
      state2.byPid("p1").seqNr shouldBe 3L
      state2.byPid("p2").seqNr shouldBe 2L
      // latest not updated because timestamp of p2 was before latest
      state2.latestTimestamp shouldBe t0.plusMillis(2)
      state2.latestOffset.get.seen shouldBe Map("p1" -> 3L)
      state2.oldestTimestamp shouldBe t0

      val state3 = state2.add(Vector(createRecord("p3", 10, t0.plusMillis(3))))
      state3.byPid("p1").seqNr shouldBe 3L
      state3.byPid("p2").seqNr shouldBe 2L
      state3.byPid("p3").seqNr shouldBe 10L
      state3.latestTimestamp shouldBe t0.plusMillis(3)
      state3.latestOffset.get.seen shouldBe Map("p3" -> 10L)
      state3.oldestTimestamp shouldBe t0
    }

    // reproducer of issue #173
    "include highest seqNr in seen of latestOffset" in {
      val t0 = TestClock.nowMillis().instant()
      val records =
        Vector(
          createRecord("p4", 9, t0),
          createRecord("p2", 2, t0),
          createRecord("p3", 5, t0),
          createRecord("p2", 1, t0),
          createRecord("p1", 3, t0))
      val state = State(records)
      state.byPid("p2").seqNr shouldBe 2L
      // p2 -> 2 should be included even though p2 -> 1 was added afterwards (same timestamp)
      state.latestOffset.get.seen shouldBe Map("p1" -> 3L, "p2" -> 2L, "p3" -> 5L, "p4" -> 9L)
    }

    "evict old" in {
      // these pids have the same slice 645, otherwise it will keep one for each slice
      val p1 = "p500"
      val p2 = "p621"
      val p3 = "p742"
      val p4 = "p863"
      val p5 = "p984"

      val t0 = TestClock.nowMillis().instant()
      val state1 = State.empty
        .add(
          Vector(
            createRecord(p1, 1, t0),
            createRecord(p2, 2, t0.plusMillis(1)),
            createRecord(p3, 3, t0.plusMillis(2)),
            createRecord(p4, 4, t0.plusMillis(3)),
            createRecord(p5, 5, t0.plusMillis(4))))
      state1.latestOffset.get.seen shouldBe Map(p5 -> 5L)
      state1.oldestTimestamp shouldBe t0
      state1.byPid
        .map { case (pid, r) => pid -> r.seqNr } shouldBe Map(p1 -> 1L, p2 -> 2L, p3 -> 3L, p4 -> 4L, p5 -> 5L)

      val state2 = state1.evict(t0.plusMillis(2), keepNumberOfEntries = 1)
      state2.latestOffset.get.seen shouldBe Map(p5 -> 5L)
      state2.oldestTimestamp shouldBe t0.plusMillis(2)
      state2.byPid.map { case (pid, r) => pid -> r.seqNr } shouldBe Map(p3 -> 3L, p4 -> 4L, p5 -> 5L)

      // keep all
      state1.evict(t0.plusMillis(2), keepNumberOfEntries = 100) shouldBe state1

      // keep 4
      val state3 = state1.evict(t0.plusMillis(2), keepNumberOfEntries = 4)
      state3.latestOffset.get.seen shouldBe Map(p5 -> 5L)
      state3.oldestTimestamp shouldBe t0.plusMillis(1)
      state3.byPid.map { case (pid, r) => pid -> r.seqNr } shouldBe Map(p2 -> 2L, p3 -> 3L, p4 -> 4L, p5 -> 5L)
    }

    "evict old but keep latest for each slice" in {
      val t0 = TestClock.nowMillis().instant()
      val state1 = State.empty
        .add(
          Vector(
            createRecord("p1", 1, t0),
            createRecord("p92", 2, t0.plusMillis(1)),
            createRecord("p108", 3, t0.plusMillis(2)),
            createRecord("p4", 4, t0.plusMillis(3)),
            createRecord("p5", 5, t0.plusMillis(4))))

      state1.byPid("p1").slice shouldBe 449
      state1.byPid("p92").slice shouldBe 905
      state1.byPid("p108").slice shouldBe 905 // same slice as p92
      state1.byPid("p4").slice shouldBe 452
      state1.byPid("p5").slice shouldBe 453

      state1.byPid.map { case (pid, r) => pid -> r.seqNr } shouldBe Map(
        "p1" -> 1L,
        "p92" -> 2L,
        "p108" -> 3L,
        "p4" -> 4L,
        "p5" -> 5L)
      state1.latestOffset.get.seen shouldBe Map("p5" -> 5L)
      state1.oldestTimestamp shouldBe t0

      val state2 = state1.evict(t0.plusMillis(2), keepNumberOfEntries = 1)
      // note that p92 is evicted because it has same slice as p108
      // p1 is kept because keeping one for each slice
      state2.byPid
        .map { case (pid, r) => pid -> r.seqNr } shouldBe Map("p1" -> 1L, "p108" -> 3L, "p4" -> 4L, "p5" -> 5L)
      state2.latestOffset.get.seen shouldBe Map("p5" -> 5L)
      state2.oldestTimestamp shouldBe t0

      val state3 = state1.evict(t0.plusMillis(10), keepNumberOfEntries = 1)
      // still keeping one for each slice
      state3.byPid
        .map { case (pid, r) => pid -> r.seqNr } shouldBe Map("p1" -> 1L, "p108" -> 3L, "p4" -> 4L, "p5" -> 5L)
    }

    "find duplicate" in {
      val t0 = TestClock.nowMillis().instant()
      val state =
        State(
          Vector(
            createRecord("p1", 1, t0),
            createRecord("p2", 2, t0.plusMillis(1)),
            createRecord("p3", 3, t0.plusMillis(2))))
      state.isDuplicate(createRecord("p1", 1, t0)) shouldBe true
      state.isDuplicate(createRecord("p1", 2, t0.plusMillis(10))) shouldBe false
      state.isDuplicate(createRecord("p2", 1, t0)) shouldBe true
      state.isDuplicate(createRecord("p4", 4, t0.plusMillis(10))) shouldBe false
    }
  }
}
