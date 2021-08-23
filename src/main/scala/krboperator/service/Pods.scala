package krboperator.service

import cats.effect.{Async, Temporal}
import cats.implicits._

import java.io.ByteArrayOutputStream
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.{FiniteDuration, _}
import scala.jdk.CollectionConverters._
import scala.util.Using
import com.goyeau.kubernetes.client.KubernetesClient
import com.goyeau.kubernetes.client.api.ExecStream
import com.goyeau.kubernetes.client.api.ExecStream.{StdErr, StdOut}
import io.k8s.api.core.v1.Pod
import io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta
import org.typelevel.log4cats.Logger
import krboperator.LoggingUtils
import ServiceUtils._

trait PodsAlg[F[_]] {

  def executeInPod(client: KubernetesClient[F], containerName: String)(
      namespace: String,
      podName: String,
      commands: List[String]
  ): F[Unit]

  def findPod(
      client: KubernetesClient[F]
  )(namespace: String, label: (String, String)): F[Option[Pod]]

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

  def executeInPod(client: KubernetesClient[F], containerName: String)(
      namespace: String,
      podName: String,
      commands: List[String]
  ): F[Unit] = {
    val execution = client.pods
      .namespace(namespace)
      .exec(
        podName = podName,
        container = Some(containerName),
        command = commands,
        flow = (es: ExecStream) => List(es)
      )

    for {
      (messages, status) <- execution
      success = status.exists(s => s.status == Some("Success"))
      errors = messages.collect { case StdErr(d) => d }
      res <-
        if (success && errors.isEmpty) {
          val out = messages.collect { case StdOut(d) => d }
          debug(
            namespace,
            s"Successfully executed in $namespace/$podName/$containerName stdout:\n$out"
          )
        } else
          F.raiseError(
            new RuntimeException(
              s"Failed to exec into $namespace/$podName/$containerName, status = $status, error(s): $errors"
            )
          )
    } yield res
  }

  def findPod(
      client: KubernetesClient[F]
  )(namespace: String, label: (String, String)): F[Option[Pod]] =
    client.pods
      .namespace(namespace)
      .list(Map(label))
      .map(
        _.items.find(_.metadata.flatMap(_.deletionTimestamp).isEmpty)
      )

  def waitForPod(client: KubernetesClient[F])(
      meta: ObjectMeta,
      previewPod: Option[Pod] => F[Unit],
      findPod: F[Option[Pod]],
      duration: FiniteDuration = 1.minute
  ): F[Option[Pod]] =
    for {
      ns <- getNamespace(meta)
      _ <-
        debug(
          ns,
          s"Going to wait for Pod in namespace $ns until it is ready for max $duration"
        )
      (ready, pod) <- waitFor[F, Pod](ns, duration, previewPod) {
        for {
          p <- findPod
          ready <- p match {
            case Some(pod) => isPodReady(pod).pure[F]
            case None      => false.pure[F]
          }
        } yield (ready, p)
      }
      _ <- F.whenA(ready)(
        debug(ns, s"POD in namespace ${meta.namespace} is ready ")
      )
    } yield pod

  private def isPodReady(pod: Pod): Boolean =
    pod.status
      .flatMap(
        _.conditions
          .flatMap(
            _.find(condition =>
              condition.`type` == "Ready" && condition.status == "True"
            )
          )
      )
      .isDefined
}
