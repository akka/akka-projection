package akka.projection.scaladsl.kafka

import akka.{Done, NotUsed}
import akka.kafka.ConsumerMessage.{Committable, CommittableMessage}
import akka.kafka.{CommitterSettings, ConsumerSettings, Subscription}
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.scaladsl.{Flow, Source}

import scala.concurrent.{ExecutionContext, Future}

// Either users need to build their own at-least-once Kafka source or we provide this convenience in Alpakka
object KafkaConsumer {

  def atLeastOnce[K, V](settings: ConsumerSettings[K, V],
                        subscription: Subscription,
                        committerSettings: CommitterSettings)(handler: CommittableMessage[K, V] => Future[_])(implicit ec: ExecutionContext): Source[Done, Control] = {
   Consumer
      .committableSource(settings, subscription)
      .mapAsync(1) { msg => handler(msg).map( _ => msg.committableOffset) }
      .via(Committer.flow(committerSettings))
  }

}
