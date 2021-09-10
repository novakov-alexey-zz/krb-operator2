package krboperator.service

import cats.effect.{Async, Sync, Temporal}
import cats.implicits._
import com.goyeau.kubernetes.client.KubernetesClient
import io.k8s.api.apps.v1.Deployment
import io.k8s.api.core.v1._
import io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta
import krboperator.service.ServiceUtils._
import krboperator.service.Template._
import krboperator.{KrbOperatorCfg, LoggingUtils}
import org.http4s.Status
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

trait DeploymentResourceAlg[F[_]] {
  def delete(client: KubernetesClient[F], resource: Deployment): F[Boolean]

  def findDeployment(
      client: KubernetesClient[F],
      meta: ObjectMeta
  ): F[Option[Deployment]]

  def createOrReplace(
      client: KubernetesClient[F],
      deployment: Deployment,
      meta: ObjectMeta
  ): F[Deployment]

  def isDeploymentReady(resource: Deployment): Boolean
}

class DeploymentResource[F[_]](implicit F: Sync[F])
    extends DeploymentResourceAlg[F] {

  override def delete(client: KubernetesClient[F], d: Deployment): F[Boolean] =
    client.deployments
      .namespace(d.metadata.get.namespace.get)
      .delete(d.metadata.get.name.get)
      .map(_ == Status.Ok)

  override def findDeployment(
      client: KubernetesClient[F],
      meta: ObjectMeta
  ): F[Option[Deployment]] =
    client.deployments
      .namespace(meta.namespace.get)
      .get(meta.name.get)
      .map(_.some)
      .handleError(_ => none[Deployment])

  override def createOrReplace(
      client: KubernetesClient[F],
      deployment: Deployment,
      meta: ObjectMeta
  ): F[Deployment] =
    client.deployments
      .namespace(meta.namespace.get)
      .createOrUpdate(deployment)
      .flatMap {
        case Status.Ok => F.pure(deployment)
        case s =>
          F.raiseError(
            new RuntimeException(
              s"failed to create resource $deployment, status: $s"
            )
          )
      }

  override def isDeploymentReady(resource: Deployment): Boolean = {
    val ready = for {
      spec <- resource.spec
      replicas <- spec.replicas
      status <- resource.status
      statusReplicas <- status.replicas
      availableReplicas <- status.availableReplicas
      ready = (replicas == statusReplicas && replicas <= availableReplicas)
    } yield ready
    ready.getOrElse(false)
  }
}

object Template {
  val PrefixParam = "PREFIX"
  val AdminPwdParam = "ADMIN_PWD"
  val KdcServerParam = "KDC_SERVER"
  val KrbRealmParam = "KRB5_REALM"
  val Krb5Image = "KRB5_IMAGE"
  val DeploymentSelector = "deployment"

  val deploymentTimeout: FiniteDuration = 1.minute

  object implicits {
    implicit def k8sDeployment[F[_]: Sync]: DeploymentResourceAlg[F] =
      new DeploymentResource[F]
  }
}

class Template[F[_]](
    client: KubernetesClient[F],
    secrets: Secrets[F],
    cfg: KrbOperatorCfg
)(implicit
    F: Async[F],
    T: Temporal[F],
    resource: DeploymentResourceAlg[F],
    val logger: Logger[F]
) extends WaitUtils
    with LoggingUtils[F] {

  def delete(meta: ObjectMeta): F[Unit] =
    (for {
      deployment <- findDeployment(meta)
      deleteDeployment <- deployment.fold(F.pure(false))(d =>
        resource.delete(client, d)
      )
      service <- findService(meta)
      namespace <- getNamespace(meta)
      deleteService <- service.fold(F.pure(false))(s =>
        client.services
          .namespace(namespace)
          .delete(s.metadata.get.name.get)
          .map(_ == Status.Ok)
      )
      secret <- secrets.findAdminSecret(meta)
      deleteAdminSecret <- secret.fold(F.pure(false))(s =>
        client.secrets
          .namespace(namespace)
          .delete(s.metadata.get.name.get)
          .map(_ == Status.Ok)
      )

      found =
        if (deleteDeployment || deleteService || deleteAdminSecret) "found"
        else "not found"
      _ <- info(meta.namespace.get, s"$found resources to delete")
    } yield ()).onError { case e: Throwable =>
      error(meta.namespace.get, "Failed to delete", e)
    }

  def waitForDeployment(metadata: ObjectMeta): F[Unit] =
    info(
      metadata.namespace.get,
      s"Going to wait for deployment until ready: $deploymentTimeout"
    ) *>
      waitFor[F](metadata.namespace.get, deploymentTimeout) {
        findDeployment(metadata).flatMap { d =>
          d.fold(false)(resource.isDeploymentReady).pure[F]
        }
      }.flatMap { ready =>
        if (ready)
          debug(metadata.namespace.get, s"deployment is ready: $metadata")
        else
          F.raiseError(
            new RuntimeException("Failed to wait for deployment readiness")
          )
      }

  def findDeployment(meta: ObjectMeta): F[Option[Deployment]] =
    resource.findDeployment(client, meta)

  def findService(meta: ObjectMeta): F[Option[Service]] =
    client.services
      .namespace(meta.namespace.get)
      .get(meta.name.get)
      .map(Option(_))
      .handleError(_ => None)

  def createService(meta: ObjectMeta): F[Unit] =
    for {
      name <- F.fromOption(
        meta.name,
        new RuntimeException("Metadata name is emnty!")
      )
      ns <- getNamespace(meta)
      s = Specs.service(name)
      _ <- client.services
        .namespace(ns)
        .createOrUpdate(s)
        .void
        .recoverWith { case e =>
          for {
            missing <- findService(meta).map(_.isEmpty)
            error <- F.whenA(missing)(F.raiseError(e))
          } yield error
        }
    } yield ()

  def createDeployment(meta: ObjectMeta, realm: String): F[Unit] =
    for {
      ns <- getNamespace(meta)
      _ <- debug(
        ns,
        s"Creating new deployment for KDC: ${meta.name}"
      )
      deploymentName <- F.fromOption(
        meta.name,
        new RuntimeException("Deployment name is empty")
      )
      _ <- F.delay {
        val spec = Specs.deployment(deploymentName, realm, cfg)
        resource.createOrReplace(client, spec, meta)
      }
    } yield ()
}
