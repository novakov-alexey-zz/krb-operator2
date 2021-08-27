package krboperator.service

import java.io.ByteArrayInputStream
import java.nio.file.Files
import java.util.Base64

import cats.effect.Sync
import cats.implicits._

import scala.jdk.CollectionConverters._
import io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta
import io.k8s.api.core.v1.Secret
import org.typelevel.log4cats.Logger
import com.goyeau.kubernetes.client.KubernetesClient
import krboperator.KrbOperatorCfg
import krboperator.LoggingUtils
import Secrets._
import krboperator.KeytabAndPassword
import ServiceUtils._
import scala.util.Random

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
      principals: List[PrincipalsWithKey],
      secretName: String
  ): F[Unit] =
    F.delay {
      val keytabs = principals.map(_.keytabMeta)
      debug(
        namespace,
        s"Creating secret for [${keytabs.mkString(",")}] keytabs"
      )
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

      val secret = principals
        .foldLeft(secretBase) { case (acc, principals) =>
          val bytes = Files.readAllBytes(principals.keytabMeta.path)
          acc.copy(data =
            acc.data.map(
              _ ++ Map(
                principals.keytabMeta.name ->
                  Base64.getEncoder.encodeToString(bytes)
              )
            )
          )

          val credentialsWithPassword = principals.credentials
            .filter(_.secret match {
              case KeytabAndPassword(_) => true
              case _                    => false
            })

          credentialsWithPassword
            .foldLeft(secretBase) { case (acc, c) =>
              acc.copy(data =
                acc.data.map(
                  _ ++ Map(
                    c.username -> Base64.getEncoder.encodeToString(
                      c.password.getBytes()
                    )
                  )
                )
              )
            }
        }

      client.secrets.namespace(namespace).createOrUpdate(secret)
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

  private def randomPassword = Random.alphanumeric.take(10).mkString

  private def secretSpec(name: String) =
    Secret(
      metadata = Some(
        ObjectMeta(name = Some(name))
      ),
      data = Some(Map("krb5_pass" -> randomPassword))
    )

  def createAdminSecret(meta: ObjectMeta): F[Unit] = {
    val secretName = cfg.k8sResourcesPrefix + "-krb-admin-pwd"
    val secret = secretSpec(secretName)
    for {
      ns <- getNamespace(meta)
      _ <- client.secrets.namespace(ns).createOrUpdate(secret)
    } yield ()
  }
}
