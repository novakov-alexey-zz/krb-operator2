package krboperator.service

import cats.effect.{Async, Temporal}
import cats.syntax.all._
import krboperator.LoggingUtils
import WaitUtils._

import scala.concurrent.duration._
import org.typelevel.log4cats.Logger

object WaitUtils {
  val defaultDelay: FiniteDuration = 500.millis
}

trait WaitUtils {

  def waitFor[F[_]: Logger](namespace: String, waitTime: FiniteDuration)(
      action: F[Boolean]
  )(implicit F: Async[F], T: Temporal[F]): F[Boolean] = {
    val result =
      waitFor[F, Nothing](namespace, waitTime, _ => F.unit, defaultDelay)(
        action.map(a => (a, None))
      )
    result.map { case (status, _) =>
      status
    }
  }

  def waitFor[F[_], S](
      namespace: String,
      waitTime: FiniteDuration,
      peek: Option[S] => F[Unit],
      delay: FiniteDuration = defaultDelay
  )(action: F[(Boolean, Option[S])])(implicit
      F: Async[F],
      logger: Logger[F]
  ): F[(Boolean, Option[S])] = {
    val debug = LoggingUtils.logDebugWithNamespace(logger)

    def loop(
        spent: FiniteDuration,
        waitTime: FiniteDuration
    ): F[(Boolean, Option[S])] =
      action.flatMap {
        case (true, state) => (true, state).pure[F]
        case (false, state) if spent < waitTime =>
          F.whenA(spent.toMillis != 0 && spent.toSeconds % 5 == 0) {
            peek(state) *> debug(
              namespace,
              s"Already spent time: ${spent.toSeconds} secs / $waitTime"
            )
          } *> F.sleep(delay) *> loop(spent + delay, waitTime)
        case (_, state) =>
          debug(namespace, s"Was waiting for ${spent.toMinutes} mins") *> (
            true,
            state
          ).pure[F]
      }

    loop(0.millisecond, waitTime)
  }
}
