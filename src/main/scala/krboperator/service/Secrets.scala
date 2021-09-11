package krboperator.service

import cats.effect.Sync
import cats.implicits._
import com.goyeau.kubernetes.client.KubernetesClient
import io.k8s.api.core.v1.Secret
import io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta
import krboperator.PrincipalController.DownloadStatus
import krboperator.service.Secrets._
import krboperator.service.ServiceUtils._
import krboperator.{KeytabAndPassword, KrbOperatorCfg, LoggingUtils}
import org.typelevel.log4cats.Logger

import java.nio.file.Files
import java.util.Base64

object Secrets {
  val principalSecretLabel = Map("app" -> "krb")
}

class Secrets[F[_]](client: KubernetesClient[F], cfg: KrbOperatorCfg)(implicit
    F: Sync[F],
    val logger: Logger[F]
) extends LoggingUtils[F] {

  def getAdminPwd(meta: ObjectMeta): F[String] = {
    val maybeSecret = for {
      ns <- getNamespace(meta)
      secret <- client.secrets
        .namespace(ns)
        .get(cfg.adminPwd.secretName)
        .map(s => Option(s) -> ns)
    } yield secret

    maybeSecret
      .flatMap {
        case (Some(secret), ns) =>
          val pwd = secret.data.flatMap(_.get(cfg.adminPwd.secretKey))
          pwd match {
            case Some(p) =>
              info(ns, s"Found admin password for $meta") *> F
                .pure(new String(Base64.getDecoder.decode(p)))
            case None =>
              F.raiseError[String](
                new RuntimeException(
                  s"Failed to get an admin password at key: ${cfg.adminPwd.secretKey}"
                )
              )
          }
        case (None, ns) =>
          F.raiseError[String](
            new RuntimeException(
              s"Failed to find a secret '${cfg.adminPwd.secretName}' in namespace $ns"
            )
          )
      }
      .onError { case t: Throwable =>
        error(meta.namespace.get, "Failed to get an admin password", t)
      }
  }

  def create(
      namespace: String,
      statuses: List[DownloadStatus],
      secretName: String
  ): F[Unit] = {
    val keytabs = statuses.map(_.principals.keytab)
    val secretBase = Secret(
      metadata = Some(
        ObjectMeta(
          name = Some(secretName),
          labels = Some(
            principalSecretLabel
          )
        )
      ),
      `type` = Some("opaque")
    )

    val secret = statuses
      .foldLeft(secretBase) { case (acc, status) =>
        val bytes = Files.readAllBytes(
          status.principals.keytab.localPath
            .getOrElse(status.principals.keytab.remotePath)
        )

        val secretWithKeytab = acc.copy(data =
          Some(
            acc.data.getOrElse(Map.empty[String, String]) ++ Map(
              status.principals.keytab.name ->
                Base64.getEncoder.encodeToString(bytes)
            )
          )
        )

        val credentialsWithPassword = status.principals.credentials
          .filter(_.secret match {
            case KeytabAndPassword(_) => true
            case _                    => false
          })

        credentialsWithPassword
          .foldLeft(secretWithKeytab) { case (acc, c) =>
            acc.copy(data =
              Some(
                acc.data.getOrElse(Map.empty[String, String]) ++ Map(
                  c.username -> Base64.getEncoder.encodeToString(
                    c.password.getBytes()
                  )
                )
              )
            )
          }
      }

    for {
      _ <- debug(
        namespace,
        s"Creating secret for [${keytabs.mkString(",")}] keytab(s)"
      )
      status <- client.secrets.namespace(namespace).createOrUpdate(secret)
      _ <- F.whenA(!status.isSuccess)(
        F.raiseError(
          new RuntimeException(
            s"Failed to create secret, cause: ${status.toString}"
          )
        )
      )
    } yield ()
  }

  def delete(namespace: String): F[Unit] =
    client.secrets.namespace(namespace).deleteAll(principalSecretLabel).void

  def findMissing(
      meta: ObjectMeta,
      expectedSecrets: Set[String]
  ): F[Set[String]] = for {
    ns <- getNamespace(meta)
    _ <- debug(
      ns,
      s"Expected secrets to find: ${expectedSecrets.mkString(",")}"
    )
    missing <- client.secrets
      .namespace(ns)
      .list(principalSecretLabel)
      .map(Some(_))
      .flatMap {
        case Some(secretList) =>
          val foundSecrets = secretList.items
            .flatMap(_.metadata.flatMap(_.name))
            .toSet
          F.pure(expectedSecrets -- foundSecrets)
        case _ =>
          F.pure(expectedSecrets)
      }
  } yield missing

  def findAdminSecret(meta: ObjectMeta): F[Option[Secret]] =
    for {
      ns <- getNamespace(meta)
      secret <- client.secrets
        .namespace(ns)
        .get(cfg.adminPwd.secretName)
        .map(Option(_))
        .handleError(_ => None)
    } yield secret

  def createAdminSecret(meta: ObjectMeta): F[Unit] = {
    val secretName = cfg.k8sResourcesPrefix + "-krb-admin-pwd"
    val secret = Specs.adminSecret(secretName)
    for {
      ns <- getNamespace(meta)
      status <- client.secrets.namespace(ns).createOrUpdate(secret)
      _ <- F.whenA(!status.isSuccess)(
        F.raiseError(
          new RuntimeException(
            s"Failed to create admin secret, cause: ${status.toString}"
          )
        )
      )
    } yield ()
  }
}
