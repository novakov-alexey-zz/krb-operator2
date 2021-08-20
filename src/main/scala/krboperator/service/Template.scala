package krboperator.service

import cats.effect.{Async, Sync, Temporal}
import cats.implicits._

import java.io.ByteArrayInputStream
import java.nio.file.{Path, Paths}
import scala.concurrent.duration._
import scala.io.Source
import scala.util.{Random, Using}
import io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta
import io.k8s.api.apps.v1.Deployment
import io.k8s.api.core.v1.Service
import com.goyeau.kubernetes.client.KubernetesClient
import org.http4s.Status
import org.typelevel.log4cats.Logger

import krboperator.LoggingUtils
import krboperator.KrbOperatorCfg
import Template._

object Template {
  val PrefixParam = "PREFIX"
  val AdminPwdParam = "ADMIN_PWD"
  val KdcServerParam = "KDC_SERVER"
  val KrbRealmParam = "KRB5_REALM"
  val Krb5Image = "KRB5_IMAGE"
  val DeploymentSelector = "deployment"

  val deploymentTimeout: FiniteDuration = 1.minute
}

trait DeploymentResourceAlg[F[_]] {
  def delete(client: KubernetesClient[F], resource: Deployment): F[Boolean]

  def findDeployment(
      client: KubernetesClient[F],
      meta: ObjectMeta
  ): F[Option[Deployment]]

  def createOrReplace(
      client: KubernetesClient[F],
      is: ByteArrayInputStream,
      meta: ObjectMeta
  ): F[Deployment]

  def isDeploymentReady(resource: Deployment): F[Boolean]

  val deploymentSpecName: String
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
      .handleError(_ => None)

  override def createOrReplace(
      client: KubernetesClient[F],
      is: ByteArrayInputStream,
      meta: ObjectMeta
  ): F[Deployment] = {
    val dc: Deployment = ??? //TODO: client.deployments.load(is)

    client.deployments
      .namespace(meta.namespace.get)
      .createOrUpdate(dc)
      .flatMap { s =>
        s match {
          case Status.Ok => F.pure(dc)
          case _ =>
            F.raiseError(
              new RuntimeException(s"failed to create resource $dc, status $s")
            )
        }
      }
  }

  override val deploymentSpecName: String = "krb5-deployment.yaml"

  override def isDeploymentReady(resource: Deployment): F[Boolean] =
    ???
  //TODO: check fabric8 logic in Readiness.isDeploymentReady(resource)
}

object DeploymentResource {

  implicit def k8sDeployment[F[_]: Sync]: DeploymentResource[F] =
    new DeploymentResource[F]

}

class Template[F[_], T](
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

  val adminSecretSpec: String = replaceParams(
    Paths.get(cfg.k8sSpecsDir, "krb5-admin-secret.yaml"),
    Map(PrefixParam -> cfg.k8sResourcesPrefix, AdminPwdParam -> randomPassword)
  )

  private def deploymentSpec(kdcName: String, krbRealm: String) = replaceParams(
    Paths.get(cfg.k8sSpecsDir, resource.deploymentSpecName),
    Map(
      KdcServerParam -> kdcName,
      KrbRealmParam -> krbRealm,
      Krb5Image -> cfg.krb5Image,
      PrefixParam -> cfg.k8sResourcesPrefix
    )
  )

  private def serviceSpec(kdcName: String) =
    replaceParams(
      Paths.get(cfg.k8sSpecsDir, "krb5-service.yaml"),
      Map(KdcServerParam -> kdcName)
    )

  private def replaceParams(
      pathToFile: Path,
      params: Map[String, String]
  ): String =
    Using.resource(
      Source
        .fromFile(pathToFile.toFile)
    ) {
      _.getLines()
        .map { l =>
          params.view.foldLeft(l) { case (acc, (k, v)) =>
            acc.replaceAll("\\$\\{" + k + "\\}", v)
          }
        }
        .toList
        .mkString("\n")
    }

  private def randomPassword = Random.alphanumeric.take(10).mkString

  def delete(meta: ObjectMeta): F[Unit] =
    (for {
      deployment <- findDeployment(meta)
      deleteDeployment <- deployment.fold(F.pure(false))(d =>
        resource.delete(client, d)
      )
      service <- findService(meta)
      deleteService <- service.fold(F.pure(false))(s =>
        client.services
          .namespace(meta.namespace.get)
          .delete(s.metadata.get.name.get)
          .map(_ == Status.Ok)
      )
      secret <- secrets.findAdminSecret(meta)
      deleteAdminSecret <- secret.fold(F.pure(false))(s =>
        client.secrets
          .namespace(meta.namespace.get)
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

  def waitForDeployment(metadata: ObjectMeta): F[Unit] = {
    info(
      metadata.namespace.get,
      s"Going to wait for deployment until ready: $deploymentTimeout"
    ) *>
      waitFor[F](metadata.namespace.get, deploymentTimeout) {
        findDeployment(metadata).flatMap { d =>
          d.fold(F.pure(false))(resource.isDeploymentReady)
        }
      }.flatMap { ready =>
        if (ready)
          debug(metadata.namespace.get, s"deployment is ready: $metadata")
        else
          F.raiseError(
            new RuntimeException("Failed to wait for deployment readiness")
          )
      }
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
    F.delay {
      val is = new ByteArrayInputStream(serviceSpec(meta.name.get).getBytes())
      val s: Service = ??? //TODO: client.services.load(is).get()
      client.services.namespace(meta.namespace.get).createOrUpdate(s)
    }.void
      .recoverWith { case e =>
        for {
          missing <- findService(meta).map(_.isEmpty)
          error <- F.whenA(missing)(F.raiseError(e))
        } yield error
      }

  def createDeployment(meta: ObjectMeta, realm: String): F[Unit] =
    debug(
      meta.namespace.get,
      s"Creating new deployment for KDC: ${meta.name}"
    ) *>
      F.delay {
        val content = deploymentSpec(meta.name.get, realm)
        val is = new ByteArrayInputStream(content.getBytes)
        resource.createOrReplace(client, is, meta)
      }
}
