package iot.registration

import akka.Done
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.pattern.StatusReply
import akka.persistence.testkit.scaladsl.EventSourcedBehaviorTestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AnyWordSpecLike

object RegistrationSpec {
  val config = EventSourcedBehaviorTestKit.config

}

class RegistrationSpec
    extends ScalaTestWithActorTestKit(RegistrationSpec.config)
    with AnyWordSpecLike
    with BeforeAndAfterEach {

  private val eventSourcedTestKit =
    EventSourcedBehaviorTestKit[
      Registration.Command,
      Registration.Event,
      Registration.State](system, Registration("sensor-0"))

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    eventSourcedTestKit.clear()
  }

  "Registration" should {

    "register sensor" in {
      val result1 =
        eventSourcedTestKit.runCommand[StatusReply[Done]](replyTo =>
          Registration.Register(Registration.SecretDataValue("foo"), replyTo))
      result1.reply shouldBe StatusReply.Ack
      result1.event shouldBe Registration.Registered(
        Registration.SecretDataValue("foo"))
    }

  }

}
