package shopping.cart

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.pattern.StatusReply
import akka.persistence.testkit.scaladsl.EventSourcedBehaviorTestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AnyWordSpecLike

object ShoppingCartSpec {
  val config = EventSourcedBehaviorTestKit.config

  def summary(items: Map[String, Int], checkedOut: Boolean) = ShoppingCart.Summary(items, checkedOut)
}

class ShoppingCartSpec
    extends ScalaTestWithActorTestKit(ShoppingCartSpec.config)
    with AnyWordSpecLike
    with BeforeAndAfterEach {

  import ShoppingCartSpec._

  private val eventSourcedTestKit =
    EventSourcedBehaviorTestKit[
      ShoppingCart.Command,
      ShoppingCart.Event,
      ShoppingCart.State](system, ShoppingCart("carts-0"))

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    eventSourcedTestKit.clear()
  }

  "The Shopping Cart" should {

    "add item" in {
      val result1 =
        eventSourcedTestKit.runCommand[StatusReply[ShoppingCart.Summary]](
          replyTo => ShoppingCart.AddItem("foo", 42, replyTo))
      result1.reply should ===(
        StatusReply.Success(
          summary(Map("foo" -> 42), checkedOut = false)))
      result1.event should ===(ShoppingCart.ItemUpdated("foo", 42))
    }

    "remove item" in {
      val result1 =
        eventSourcedTestKit.runCommand[StatusReply[ShoppingCart.Summary]](
          ShoppingCart.AddItem("foo", 42, _))
      result1.reply.isSuccess should ===(true)
      val result2 =
        eventSourcedTestKit.runCommand[StatusReply[ShoppingCart.Summary]](
          ShoppingCart.RemoveItem("foo", 42, _))
      result2.reply should ===(
        StatusReply.Success(summary(Map.empty, checkedOut = false)))
      result2.event should ===(ShoppingCart.ItemUpdated("foo", -42))
    }

    "adjust quantity" in {
      val result1 =
        eventSourcedTestKit.runCommand[StatusReply[ShoppingCart.Summary]](
          ShoppingCart.AddItem("foo", 42, _))
      result1.reply.isSuccess should ===(true)
      val result2 =
        eventSourcedTestKit.runCommand[StatusReply[ShoppingCart.Summary]](
          ShoppingCart.AddItem("foo", 1, _))
      result2.reply should ===(
        StatusReply.Success(
          ShoppingCart.Summary(Map("foo" -> 43), checkedOut = false)))
      result2.event should ===(
        ShoppingCart.ItemUpdated("foo", 1))
    }

    // tag::checkout[]
    "checkout" in {
      val result1 =
        eventSourcedTestKit.runCommand[StatusReply[ShoppingCart.Summary]](
          ShoppingCart.AddItem("foo", 42, _))
      result1.reply.isSuccess should ===(true)
      val result2 = eventSourcedTestKit
        .runCommand[StatusReply[ShoppingCart.Summary]](ShoppingCart.Checkout(_))
      result2.reply should ===(
        StatusReply.Success(summary(Map("foo" -> 42), checkedOut = true)))

      val result3 =
        eventSourcedTestKit.runCommand[StatusReply[ShoppingCart.Summary]](
          ShoppingCart.AddItem("bar", 13, _))
      result3.reply.isError should ===(true)
    }
    // end::checkout[]

    // tag::get[]
    "get" in {
      val result1 =
        eventSourcedTestKit.runCommand[StatusReply[ShoppingCart.Summary]](
          ShoppingCart.AddItem("foo", 42, _))
      result1.reply.isSuccess should ===(true)

      val result2 = eventSourcedTestKit.runCommand[ShoppingCart.Summary](
        ShoppingCart.Get(_))
      result2.reply should ===(
        summary(Map("foo" -> 42), checkedOut = false))
    }
    // end::get[]

    "keep its state" in {
      val result1 =
        eventSourcedTestKit.runCommand[StatusReply[ShoppingCart.Summary]](
          ShoppingCart.AddItem("foo", 42, _))
      result1.reply should ===(
        StatusReply.Success(
          summary(Map("foo" -> 42), checkedOut = false)))

      eventSourcedTestKit.restart()

      val result2 = eventSourcedTestKit.runCommand[ShoppingCart.Summary](
        ShoppingCart.Get(_))
      result2.reply should ===(
        summary(Map("foo" -> 42), checkedOut = false))
    }
  }

}
