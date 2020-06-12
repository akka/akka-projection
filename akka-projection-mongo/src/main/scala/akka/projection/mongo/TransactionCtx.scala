package akka.projection.mongo

import akka.Done
import org.mongodb.scala._

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success }

trait TransactionCtx[+T] {
  def apply(clientSession: ClientSession): Future[T]

  def map[U](f: T => U)(implicit ec: ExecutionContext): TransactionCtx[U] = { ctx =>
    apply(ctx).map(f)
  }

  def flatMap[U](f: T => TransactionCtx[U])(implicit ec: ExecutionContext): TransactionCtx[U] = { ctx =>
    apply(ctx).flatMap(f1 => f.apply(f1)(ctx))
  }
}

object TransactionCtx {
  def withSession[T](session: SingleObservable[ClientSession])(f: => TransactionCtx[T])(
      implicit ec: ExecutionContext): Future[T] =
    session.toFuture().flatMap { cs =>
      cs.startTransaction()
      f.apply(cs).transformWith {
        case Success(value) => cs.commitTransaction().toFuture().map(_ => value)
        case Failure(e) =>
          if (cs.hasActiveTransaction) {
            cs.abortTransaction().toFuture().flatMap(_ => Future.failed(e))
          } else {
            Future.failed(e)
          }
      }

    }

  def sequence[T](value: Seq[TransactionCtx[T]])(implicit ec: ExecutionContext): TransactionCtx[Seq[T]] = { ctx =>
    value.foldLeft(Future(Seq.empty[T])) { (previousFuture, next) =>
      for {
        previousResults <- previousFuture
        next <- (next).apply(ctx)
      } yield previousResults :+ next
    }
  }

}
