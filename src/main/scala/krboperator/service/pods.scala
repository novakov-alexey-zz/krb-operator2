package krboperator.service

import cats.effect.{Async, Temporal}
import cats.implicits._
import com.goyeau.kubernetes.client.KubernetesClient
import com.goyeau.kubernetes.client.api.ExecStream
import com.goyeau.kubernetes.client.api.ExecStream.{StdErr, StdOut}
import com.goyeau.kubernetes.client.api.NamespacedPodsApi.ErrorOrStatus
import io.k8s.api.core.v1.Pod
import io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta
import krboperator.LoggingUtils
import krboperator.service.ServiceUtils._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.{FiniteDuration, _}

trait PodsAlg[F[_]] {

  def executeInPod(
      client: KubernetesClient[F],
      containerName: String,
      ingoredErrors: Set[String]
  )(
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

class Pods[F[_]](implicit F: Async[F], T: Temporal[F])
    extends PodsAlg[F]
    with WaitUtils
    with LoggingUtils[F] {

  implicit val logger: Logger[F] = Slf4jLogger.getLogger[F]

  private def formatEvents(events: List[String]) =
    if (events.nonEmpty)
      ":\n" + events.zipWithIndex
        .map { case (e, i) =>
          s"[$i]$e"
        }
        .mkString("\n")
    else " is empty"

    def executeInPod(
        client: KubernetesClient[F],
        containerName: String,
        ingoredErrorPrefixes: Set[String]
    )(
        namespace: String,
        podName: String,
        commands: List[String]
    ): F[Unit] = {
      val execution = client.pods
        .namespace(namespace)
        .exec(
          podName = podName,
          container = Some(containerName),
          command = commands
        )

      for {
        (messages, maybeStatus) <- execution
        success = maybeStatus.exists(_.exists(_.status.contains("Success")))
        errors = messages
          .collect { case e: StdErr => e.asString }
          .mkString("")
          .split("\n")
          .filterNot(ignoredErrors(_, ingoredErrorPrefixes))
          .filterNot(_.isEmpty())
          .toList

        res <-
          if (success && errors.isEmpty) {
            val out = messages
              .collect { case o: StdOut => o.asString }
              .mkString("")
              .split("\n")
              .filterNot(_.isEmpty())
              .toList
            debug(
              namespace,
              s"Successfully executed in $namespace/$podName/$containerName stdout${formatEvents(out)}"
            )
          } else
            F.raiseError {
              val errorMsg = formatEvents(errors)
              new RuntimeException(
                s"Failed to exec into $namespace/$podName/$containerName, status = $maybeStatus, stderr$errorMsg"
              )
            }
      } yield res
    }

  private def ignoredErrors(line: String, ignored: Set[String]): Boolean =    
    ignored.exists(i => line.startsWith(i))    

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
        debug(ns, s"POD in namespace ${meta.namespace} is ready")
      )
    } yield pod

  private def isPodReady(pod: Pod): Boolean =
    (for {
      status <- pod.status
      conditions <- status.conditions
      found = conditions
        .exists(c => c.`type` == "Ready" && c.status == "True")
    } yield found).getOrElse(false)
}
