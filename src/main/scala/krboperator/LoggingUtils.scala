package krboperator

import org.typelevel.log4cats.Logger
import LoggingUtils._

object LoggingUtils {
  val checkMark: String = "\u2714"

  def logDebugWithNamespace[F[_]](
      logger: Logger[F]
  ): (String, String) => F[Unit] =
    (namespace, msg) => {
      logger.debug(
        s"${AnsiColors.gr}[ns: ${namespace}]${AnsiColors.xx} $msg"
      )
    }

  def logInfoWithNamespace[F[_]](
      logger: Logger[F]
  ): (String, String) => F[Unit] =
    (namespace, msg) => {
      logger.info(
        s"${AnsiColors.gr}[ns: ${namespace}]${AnsiColors.xx} $msg"
      )
    }

  def logErrorWithNamespace[F[_]](
      logger: Logger[F]
  ): (String, String, Throwable) => F[Unit] =
    (namespace, msg, t) => {
      logger.error(t)(
        s"${AnsiColors.gr}[ns: ${namespace}]${AnsiColors.xx} $msg"
      )
    }
}

trait LoggingUtils[F[_]] {
  implicit val logger: Logger[F]

  lazy val debug = logDebugWithNamespace[F](logger)
  lazy val info = logInfoWithNamespace[F](logger)
  lazy val error = logErrorWithNamespace[F](logger)
}
