/*
 * Copyright (C) 2020-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit.javadsl

import java.time.Duration

import akka.Done
import akka.actor.typed.ActorSystem
import akka.japi.function.Effect
import akka.japi.function.Procedure
import akka.projection.Projection
import akka.projection.testkit.scaladsl
import akka.stream.testkit.TestSubscriber

import scala.jdk.DurationConverters._

object ProjectionTestKit {
  def create(system: ActorSystem[_]): ProjectionTestKit =
    new ProjectionTestKit(system)
}

final class ProjectionTestKit private[projection] (system: ActorSystem[_]) {
  private val delegate = scaladsl.ProjectionTestKit(system)

  /**
   * Run a Projection and assert its projected data using the passed assert function.
   *
   * Projection is started and stopped by the TestKit. While the projection is running, the assert function
   * will be called every 100 milliseconds until it completes without errors (no exceptions or assertion errors are thrown).
   *
   * If the assert function doesn't complete without error within 3 seconds the test will fail.
   *
   * Note: when testing a Projection with this method, the Restart Backoff is disabled.
   * Any backoff configuration settings from `.conf` file or programmatically added will be overwritten.
   *
   * @param projection - the Projection to run
   * @param assertFunction - a function that exercises the test assertions
   */
  def run(projection: Projection[_], assertFunction: Effect): Unit =
    delegate.run(projection)(assertFunction.apply())

  /**
   * Run a Projection and assert its projected data using the passed assert function and the max duration of the test.
   *
   * Projection is started and stopped by the TestKit. While the projection is running, the assert function
   * will be called every 100 milliseconds until it completes without errors (no exceptions or assertion errors are thrown).
   *
   * If the assert function doesn't complete without error within the passed `max` duration the test will fail.
   *
   * Note: when testing a Projection with this method, the Restart Backoff is disabled.
   * Any backoff configuration settings from `.conf` file or programmatically added will be overwritten.
   *
   * @param projection - the Projection to run
   * @param max - Duration delimiting the max duration of the test
   * @param assertFunction - a function that exercises the test assertions
   */
  def run(projection: Projection[_], max: Duration, assertFunction: Effect): Unit =
    delegate.run(projection, max.toScala)(assertFunction.apply())

  /**
   * Run a Projection and assert its projected data using the passed assert function,
   * the max duration of the test and the interval between each assertion.
   *
   * Projection is started and stopped by the TestKit. While the projection is running, the assert function
   * will be called every `interval` until it completes without errors (no exceptions or assertion errors are thrown).
   *
   * If the assert function doesn't complete without error within the passed `max` duration the test will fail.
   *
   * Note: when testing a Projection with this method, the Restart Backoff is disabled.
   * Any backoff configuration settings from `.conf` file or programmatically added will be overwritten.
   *
   * @param projection - the Projection to run
   * @param max - Duration delimiting the max duration of the test
   * @param interval - Duration defining the interval in each the assert function will be called
   * @param assertFunction - a function that exercises the test assertions
   */
  def run(projection: Projection[_], max: Duration, interval: Duration, assertFunction: Effect): Unit =
    delegate.run(projection, max.toScala, interval.toScala)(assertFunction.apply())

  /**
   * Run a Projection with an attached `TestSubscriber.Probe` allowing
   * control over the pace in which the elements flow through the Projection.
   *
   * The assertion function receives a `TestSubscriber.Probe` that you can use
   * to request elements.
   *
   * The Projection starts as soon as the first element is requested by the `TestSubscriber.Probe`, new elements will be emitted
   * as requested. The Projection is stopped once the assert function completes.
   *
   * @param projection - the Projection to run
   * @param assertFunction - a function receiving a `TestSubscriber.Probe[Done]`
   */
  def runWithTestSink(projection: Projection[_], assertFunction: Procedure[TestSubscriber.Probe[Done]]): Unit =
    delegate.runWithTestSink(projection)(probe => assertFunction.apply(probe))

}
