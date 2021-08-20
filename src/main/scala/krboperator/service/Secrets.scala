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

object Secrets {
  val principalSecretLabel: Map[String, String] = Map("app" -> "krb")
}

class Secrets[F[_]](client: KubernetesClient[F], cfg: KrbOperatorCfg)(implicit
    F: Sync[F],
    val logger: Logger[F]
) extends LoggingUtils[F] {

  def getAdminPwd(meta: ObjectMeta): F[String] =
    client.secrets
      .namespace(meta.namespace.get)
      .get(cfg.adminPwd.secretName)
      .map(Option(_))
      .flatMap {
        case Some(s) =>
          val pwd = s.data.flatMap(_.get(cfg.adminPwd.secretKey))
          pwd match {
            case Some(p) =>
              info(meta.namespace.get, s"Found admin password for $meta") *> F
                .pure(new String(Base64.getDecoder.decode(p)))
            case None =>
              F.raiseError[String](
                new RuntimeException(
                  s"Failed to get an admin password at key: ${cfg.adminPwd.secretKey}"
                )
              )
          }
        case None =>
          F.raiseError[String](
            new RuntimeException(
              s"Failed to find a secret '${cfg.adminPwd.secretName}'"
            )
          )
      }
      .onError { case t: Throwable =>
        error(meta.namespace.get, "Failed to get an admin password", t)
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
      val builder = Secret(
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
        .foldLeft(builder) { case (acc, principals) =>
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
            .foldLeft(builder) { case (acc, c) =>
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
  ): F[Set[String]] = {
    debug(
      meta.namespace.get,
      s"Expected secrets to find: ${expectedSecrets.mkString(",")}"
    ) *> client.secrets
      .namespace(meta.namespace.get)
      .list(principalSecretLabel)
      .map(Some(_))
      .flatMap {
        case Some(l) =>
          val foundSecrets = l.items
            .map(_.metadata.flatMap(_.name).get)
            .toSet
          F.pure(expectedSecrets -- foundSecrets)
        case _ =>
          F.pure(expectedSecrets)
      }
  }

  def findAdminSecret(meta: ObjectMeta): F[Option[Secret]] =
    client.secrets
      .namespace(meta.namespace.get)
      .get(cfg.adminPwd.secretName)
      .map(Option(_))
      .handleError(_ => None)

  def createAdminSecret(meta: ObjectMeta, adminPwd: String): F[Unit] = {
    val secretName = cfg.k8sResourcesPrefix + "-krb-admin-pwd"
    val secret = Secret(
      metadata = Some(
        ObjectMeta(name = Some(secretName))
      ),
      data = Some(Map("krb5_pass" -> adminPwd))
    )
    client.secrets.namespace(meta.namespace.get).createOrUpdate(secret).void
  }

}
