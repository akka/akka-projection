/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.grpc.replication.internal

import java.time.Duration
import java.util.concurrent.CompletionStage

import scala.compat.java8.FutureConverters._
import scala.concurrent.Future

import akka.actor.ActorRefProvider
import akka.actor.typed.ActorRef
import akka.actor.typed.Scheduler
import akka.actor.typed.internal.InternalRecipientRef
import akka.actor.typed.scaladsl.AskPattern._
import akka.annotation.InternalApi
import akka.cluster.sharding.typed.javadsl
import akka.cluster.sharding.typed.scaladsl.EntityRef
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.japi.function.{ Function => JFunction }
import akka.pattern.StatusReply
import akka.util.JavaDurationConverters._
import akka.util.Timeout

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class NoEnvelopeEntityRef[-M](
    override val typeKey: EntityTypeKey[M],
    override val dataCenter: Option[String],
    shardRegion: ActorRef[M])(implicit scheduler: Scheduler)
    extends javadsl.EntityRef[M]
    with EntityRef[M]
    with InternalRecipientRef[M] {

  override def entityId: String = ""

  override def tell(msg: M): Unit =
    shardRegion ! msg

  override def ask[Res](f: ActorRef[Res] => M)(implicit timeout: Timeout): Future[Res] =
    shardRegion.ask(f(_))

  override def ask[U](message: JFunction[ActorRef[U], M], timeout: Duration): CompletionStage[U] =
    ask[U](replyTo => message.apply(replyTo))(timeout.asScala).toJava

  override def askWithStatus[Res](f: ActorRef[StatusReply[Res]] => M)(implicit timeout: Timeout): Future[Res] =
    shardRegion.askWithStatus(f(_))

  override def askWithStatus[Res](f: ActorRef[StatusReply[Res]] => M, timeout: Duration): CompletionStage[Res] =
    askWithStatus(f.apply)(timeout.asScala).toJava

  override private[akka] def asJava: javadsl.EntityRef[M] = this

  // impl InternalRecipientRef
  override def provider: ActorRefProvider = {
    shardRegion.asInstanceOf[InternalRecipientRef[_]].provider
  }

  // impl InternalRecipientRef
  def isTerminated: Boolean = {
    shardRegion.asInstanceOf[InternalRecipientRef[_]].isTerminated
  }
}
