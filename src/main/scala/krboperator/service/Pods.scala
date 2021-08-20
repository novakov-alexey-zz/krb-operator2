package krboperator.service

import cats.effect.{Async, Temporal}
import cats.implicits._

import java.io.ByteArrayOutputStream
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.{FiniteDuration, _}
import scala.jdk.CollectionConverters._
import scala.util.Using
import com.goyeau.kubernetes.client.KubernetesClient
import io.k8s.api.core.v1.Pod
import io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta
import org.typelevel.log4cats.Logger
import krboperator.LoggingUtils

trait PodsAlg[F[_]] {

  def executeInPod(client: KubernetesClient[F], containerName: String)(
      namespace: String,
      podName: String
  )
  //(
  //     commands: Execable[String, ExecWatch] => List[ExecWatch]
  // )
      : F[Unit]

  def getPod(
      client: KubernetesClient[F]
  )(namespace: String, labelKey: String, labelValue: String): Option[Pod]

  def waitForPod(client: KubernetesClient[F])(
      meta: ObjectMeta,
      previewPod: Option[Pod] => F[Unit],
      findPod: F[Option[Pod]],
      duration: FiniteDuration = 1.minute
  ): F[Option[Pod]]
}

object PodsAlg {
  implicit def k8sPod[F[_]: Async: Temporal: Logger]: PodsAlg[F] = new Pods[F]
}

class Pods[F[_]](implicit F: Async[F], T: Temporal[F], val logger: Logger[F])
    extends PodsAlg[F]
    with WaitUtils
    with LoggingUtils[F] {

  // private def listener(namespace: String, closed: AtomicBoolean) =
  //   new ExecListener {
  //     override def onOpen(response: Response): Unit =
  //       debug(namespace, s"on open: ${response.body().string()}")

  //     override def onFailure(t: Throwable, response: Response): Unit =
  //       error(
  //         namespace,
  //         s"Failure on 'pod exec': ${response.body().string()}",
  //         t
  //       )

  //     override def onClose(code: Int, reason: String): Unit = {
  //       debug(namespace, s"listener closed with code '$code', reason: $reason")
  //       closed.getAndSet(true)
  //     }
  //   }

  def executeInPod(client: KubernetesClient[F], containerName: String)(
      namespace: String,
      podName: String
  )
  // (
  //     commands: Execable[String, ExecWatch] => List[ExecWatch]
  // )
      : F[Unit] = ???
  // {
  //   for {
  //     (exitCode, errStreamArr) <- F.defer {
  //       val errStream = new ByteArrayOutputStream()
  //       val errChannelStream = new ByteArrayOutputStream()
  //       val isClosed = new AtomicBoolean(false)
  //       val execablePod =
  //         client
  //           .pods()
  //           .inNamespace(namespace)
  //           .withName(podName)
  //           .inContainer(containerName)
  //           .readingInput(System.in)
  //           .writingOutput(System.out)
  //           .writingError(errStream)
  //           .writingErrorChannel(errChannelStream)
  //           .withTTY()
  //           .usingListener(listener(namespace, isClosed))

  //       val watchers = commands(execablePod)

  //       for {
  //         _ <- F.delay(
  //           info(
  //             namespace,
  //             s"Waiting $ExecInPodTimeout for Pod exec listener to be closed"
  //           )
  //         )
  //         closed <- waitFor[F](namespace, ExecInPodTimeout)(
  //           F.delay(isClosed.get())
  //         )
  //         _ <- F
  //           .raiseError[Unit](
  //             new RuntimeException(
  //               s"Failed to close POD exec listener within $ExecInPodTimeout"
  //             )
  //           )
  //           .whenA(!closed)
  //         _ <- closeExecWatchers(namespace, watchers: _*)
  //         r <- F.delay {
  //           val ec = getExitCode(errChannelStream)
  //           val errStreamArr = errStream.toByteArray
  //           (ec, errStreamArr)
  //         }
  //       } yield r
  //     }
  //     checked <- checkExitCode(namespace, exitCode, errStreamArr)
  //   } yield checked
  // }

  private def getExitCode(
      errChannelStream: ByteArrayOutputStream
  ): Either[String, Int] = ???
  //   {
  //   val status =
  //     Serialization.unmarshal(errChannelStream.toString, classOf[Status])
  //   Option(status.getStatus) match {
  //     case Some("Success") => Right(0)
  //     case None            => Left("Status is null")
  //     case Some(_)         => Left(status.getMessage)
  //   }
  // }

  private def checkExitCode(
      namespace: String,
      exitCode: Either[String, Int],
      errStreamArr: Array[Byte]
  ): F[Unit] = {
    exitCode match {
      case Left(e) => F.raiseError[Unit](new RuntimeException(e))
      case _ if errStreamArr.nonEmpty =>
        val e = new String(errStreamArr)
        val t = new RuntimeException(e)
        error(namespace, s"Got error from error stream", t)
        F.raiseError[Unit](t)
      case _ => F.unit
    }
  }

  // private def closeExecWatchers(namespace: String, execs: ExecWatch*): F[Unit] =
  //   F.delay {
  //     val closedCount = execs.foldLeft(0) { case (acc, ew) =>
  //       Using.resource(ew) { _ =>
  //         acc + 1
  //       }
  //     }
  //     debug(namespace, s"Closed execWatcher(s): $closedCount")
  //   }

  def getPod(
      client: KubernetesClient[F]
  )(namespace: String, labelKey: String, labelValue: String): Option[Pod] = ???
  // client
  //   .pods
  //   .namespace(namespace)
  //   .list(Map(labelKey -> labelValue))
  //   .items
  //   .find(p => Option(p.getMetadata.getDeletionTimestamp).isEmpty)

  def waitForPod(client: KubernetesClient[F])(
      meta: ObjectMeta,
      previewPod: Option[Pod] => F[Unit],
      findPod: F[Option[Pod]],
      duration: FiniteDuration = 1.minute
  ): F[Option[Pod]] = {
    for {
      ns <- F.fromOption(
        meta.namespace,
        new RuntimeException(s"Namespace is empty in $meta")
      )
      _ <- F.delay(
        debug(
          ns,
          s"Going to wait for Pod in namespace $ns until ready: $duration"
        )
      )
      (ready, pod) <- waitFor[F, Pod](ns, duration, previewPod) {
        for {
          p <- findPod
          ready <- p match {
            case Some(pod) =>
              false.pure[F] // TODO: F.delay(Readiness.isPodReady(pod))
            case None => false.pure[F]
          }
        } yield (ready, p)
      }
      _ <- F.whenA(ready)(
        F.delay(
          debug(ns, s"POD in namespace ${meta.namespace} is ready ")
        )
      )
    } yield pod
  }
}
