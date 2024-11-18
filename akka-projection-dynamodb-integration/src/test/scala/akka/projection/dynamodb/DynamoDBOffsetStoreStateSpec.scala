/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb

import java.time.{ Duration => JDuration }
import java.time.Instant

import akka.persistence.query.TimestampOffset
import akka.projection.dynamodb.internal.DynamoDBOffsetStore.Pid
import akka.projection.dynamodb.internal.DynamoDBOffsetStore.Record
import akka.projection.dynamodb.internal.DynamoDBOffsetStore.SeqNr
import akka.projection.dynamodb.internal.DynamoDBOffsetStore.State
import org.scalatest.TestSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DynamoDBOffsetStoreStateSpec extends AnyWordSpec with TestSuite with Matchers {

  def createRecord(pid: Pid, seqNr: SeqNr, timestamp: Instant): Record = {
    Record(slice(pid), pid, seqNr, timestamp)
  }

  def slice(pid: Pid): Int = math.abs(pid.hashCode % 1024)

  "DynamoDBOffsetStore.State" should {
    "add records and keep track of pids and latest offset" in {
      val t0 = TestClock.nowMillis().instant()
      val state1 = State.empty
        .add(
          Vector(
            createRecord("p1", 1, t0),
            createRecord("p1", 2, t0.plusMillis(1)),
            createRecord("p1", 3, t0.plusMillis(2))))
      state1.byPid("p1").seqNr shouldBe 3L
      state1.offsetBySlice(slice("p1")) shouldBe TimestampOffset(t0.plusMillis(2), Map("p1" -> 3L))
      state1.latestTimestamp shouldBe t0.plusMillis(2)

      val state2 = state1.add(Vector(createRecord("p2", 2, t0.plusMillis(1))))
      state2.byPid("p1").seqNr shouldBe 3L
      state2.byPid("p2").seqNr shouldBe 2L
      slice("p2") should not be slice("p1")
      state2.offsetBySlice(slice("p2")) shouldBe TimestampOffset(t0.plusMillis(1), Map("p2" -> 2L))
      // latest not updated because timestamp of p2 was before latest
      state2.latestTimestamp shouldBe t0.plusMillis(2)

      val state3 = state2.add(Vector(createRecord("p3", 10, t0.plusMillis(3))))
      state3.byPid("p1").seqNr shouldBe 3L
      state3.byPid("p2").seqNr shouldBe 2L
      state3.byPid("p3").seqNr shouldBe 10L
      slice("p3") should not be slice("p1")
      slice("p3") should not be slice("p2")
      state3.offsetBySlice(slice("p3")) shouldBe TimestampOffset(t0.plusMillis(3), Map("p3" -> 10L))
      state3.latestTimestamp shouldBe t0.plusMillis(3)

      // same slice and same timestamp, keep both in seen
      slice("p10084") shouldBe slice("p3")
      val state4 = state3.add(Vector(createRecord("p10084", 9, t0.plusMillis(3))))
      state4.offsetBySlice(slice("p10084")) shouldBe TimestampOffset(t0.plusMillis(3), Map("p3" -> 10L, "p10084" -> 9))
    }

    "evict old" in {
      val p1 = "p500" // slice 645
      val p2 = "p621" // slice 645
      val p3 = "p742" // slice 645
      val p4 = "p863" // slice 645
      val p5 = "p984" // slice 645
      val p6 = "p92" // slice 905
      val p7 = "p108" // slice 905

      val t0 = TestClock.nowMillis().instant()
      val state1 = State.empty
        .add(
          Vector(
            createRecord(p1, 1, t0.plusMillis(1)),
            createRecord(p2, 2, t0.plusMillis(2)),
            createRecord(p3, 3, t0.plusMillis(3)),
            createRecord(p4, 4, t0.plusMillis(4)),
            createRecord(p6, 6, t0.plusMillis(6))))
      state1.byPid
        .map { case (pid, r) => pid -> r.seqNr } shouldBe Map(p1 -> 1L, p2 -> 2L, p3 -> 3L, p4 -> 4L, p6 -> 6L)

      // keep all
      state1.evict(slice = 645, timeWindow = JDuration.ofMillis(1000)) shouldBe state1

      // evict older than time window
      val state2 = state1.evict(slice = 645, timeWindow = JDuration.ofMillis(2))
      state2.byPid.map { case (pid, r) => pid -> r.seqNr } shouldBe Map(p2 -> 2L, p3 -> 3L, p4 -> 4L, p6 -> 6L)

      val state3 = state1.add(Vector(createRecord(p5, 5, t0.plusMillis(100)), createRecord(p7, 7, t0.plusMillis(10))))
      val state4 = state3.evict(slice = 645, timeWindow = JDuration.ofMillis(2))
      state4.byPid.map { case (pid, r) => pid -> r.seqNr } shouldBe Map(p5 -> 5L, p6 -> 6L, p7 -> 7L)

      val state5 = state3.evict(slice = 905, timeWindow = JDuration.ofMillis(2))
      state5.byPid.map { case (pid, r) => pid -> r.seqNr } shouldBe Map(
        p1 -> 1L,
        p2 -> 2L,
        p3 -> 3L,
        p4 -> 4L,
        p5 -> 5L,
        p7 -> 7L)
    }

    "find duplicate" in {
      val t0 = TestClock.nowMillis().instant()
      val state =
        State.empty.add(
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
