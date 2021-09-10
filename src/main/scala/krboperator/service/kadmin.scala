package krboperator.service

import cats.effect.{Async, Temporal}
import cats.implicits._
import com.goyeau.kubernetes.client.KubernetesClient
import io.k8s.api.core.v1.Pod
import io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta
import krboperator.service.ServiceUtils._
import krboperator._
import org.typelevel.log4cats.Logger

import java.nio.file.{Path, Paths}
import scala.concurrent.duration._
import scala.util.Random

final case class Credentials(
    username: String,
    password: String,
    secret: Secret
) {
  override def toString: String = s"Credentials($username, <hidden>)"
}

final case class PrincipalsWithKey(
    credentials: List[Credentials],
    keytabMeta: KeytabMeta
)
final case class KeytabMeta(name: String, path: Path)
final case class KerberosState(
    podName: String,
    principals: List[PrincipalsWithKey]
)
final case class KadminContext(
    krbServerName: String,
    realm: String,
    meta: ObjectMeta,
    adminPwd: String
)

object Kadmin {
  val ExecInPodTimeout: FiniteDuration = 60.seconds
}

trait KeytabPathGen {
  def keytabToPath(prefix: String, name: String): String
}

object KeytabPathGen {
  implicit val pathGen: KeytabPathGen = new KeytabPathGen {
    def keytabToPath(prefix: String, name: String): String =
      s"/tmp/$prefix/$name"
  }
}

class Kadmin[F[_]](client: KubernetesClient[F], cfg: KrbOperatorCfg)(implicit
    F: Async[F],
    T: Temporal[F],
    pods: PodsAlg[F],
    pathGen: KeytabPathGen,
    val logger: Logger[F]
) extends WaitUtils
    with LoggingUtils[F] {

  private lazy val executeInKadmin =
    pods.executeInPod(client, cfg.kadminContainer) _

  def createPrincipalsAndKeytabs(
      principals: List[Principal],
      context: KadminContext
  ): F[KerberosState] =
    (for {
      namespace <- getNamespace(context.meta)
      pod <- waitForPod(context)
      podName = pod.metadata.get.name.get

      principals <- {
        val groupedByKeytab = principals.groupBy(_.keytab)
        val keytabs = groupedByKeytab.toList.map { case (keytab, principals) =>
          val path = Paths.get(pathGen.keytabToPath(randomString, keytab))
          for {
            _ <- createWorkingDir(
              namespace,
              podName,
              path.getParent
            )
            credentials = principals.map(p =>
              Credentials(p.name, getPassword(p.password), p.secret)
            )
            _ <- addKeytab(context, path, credentials, podName)
          } yield PrincipalsWithKey(credentials, KeytabMeta(keytab, path))
        }
        keytabs.sequence
      }
    } yield {
      debug(namespace, s"principals created: $principals")
      KerberosState(podName, principals)
    }).adaptErr { case t =>
      new RuntimeException(
        s"Failed to create Kerberos principal(s) and keytab(s) via 'kadmin' CLI",
        t
      )
    }

  private def waitForPod(
      context: KadminContext,
      duration: FiniteDuration = 1.minute
  ): F[Pod] = {
    val label = Template.DeploymentSelector -> context.krbServerName
    val previewPod: String => Option[Pod] => F[Unit] = namespace =>
      pod =>
        pod.fold(
          debug(
            namespace,
            s"Pod with label '$label' is not available yet"
          )
        )(p =>
          debug(
            namespace,
            s"Pod '${p.metadata.map(_.name)}' is not yet ready"
          )
        )

    for {
      namespace <- getNamespace(context.meta)
      pod <- pods
        .waitForPod(client)(
          context.meta,
          previewPod(namespace),
          pods.findPod(client)(namespace, label),
          duration
        )
        .flatMap(maybePod =>
          F.fromOption(
            maybePod,
            new RuntimeException(
              s"[${context.meta.namespace}] No Pod found with label '$label'"
            )
          )
        )
    } yield pod
  }

  private def addKeytab(
      context: KadminContext,
      keytabPath: Path,
      credentials: List[Credentials],
      podName: String
  ): F[Unit] = {
    val commands = credentials.map { cred =>
      List(
        createPrincipal(context.realm, context.adminPwd, cred),
        createKeytab(context.realm, context.adminPwd, cred, keytabPath)
      )
    }

    val res = for {
      block <- commands
      command <- block
    } yield (for {
      ns <- getNamespace(context.meta)
      _ <- executeInKadmin(
        ns,
        podName,
        command
      )
    } yield ()).adaptError { case e =>
      new RuntimeException(
        s"Failed to execute command = $command in $podName pod of ${context.meta.namespace} namepspace "
      )
    }

    res.sequence_
  }

  private def createWorkingDir(
      namespace: String,
      podName: String,
      keytabDir: Path
  ): F[Unit] =
    executeInKadmin(
      namespace,
      podName,
      List("mkdir", keytabDir.toString)
    )

  def removeWorkingDir(
      namespace: String,
      podName: String,
      keytab: Path
  ): F[Unit] =
    executeInKadmin(
      namespace,
      podName,
      List("rm", "-r", keytab.getParent.toString)
    )

  private def createKeytab(
      realm: String,
      adminPwd: String,
      credentials: Credentials,
      keytab: Path
  ) = {
    val cmd = credentials.secret match {
      case KeytabAndPassword(_) => cfg.commands.addKeytab.noRandomKey
      case _                    => cfg.commands.addKeytab.randomKey
    }

    val keytabCmd = cmd
      .replaceAll("\\$realm", realm)
      .replaceAll("\\$path", keytab.toString)
      .replaceAll("\\$username", credentials.username)
    val addKeytab = s"echo '$adminPwd' | $keytabCmd"
    List("bash", "-c", addKeytab)
  }

  private def createPrincipal(
      realm: String,
      adminPassword: String,
      cred: Credentials
  ) = {
    val addCmd = cfg.commands.addPrincipal
      .replaceAll("\\$realm", realm)
      .replaceAll("\\$username", cred.username)
      .replaceAll("\\$password", cred.password)
    val addPrincipal = s"echo '$adminPassword' | $addCmd"
    List("bash", "-c", addPrincipal)
  }

  private def getPassword(password: Password): String =
    password match {
      case Static(v) => v
      case _         => randomString
    }

  private def randomString =
    Random.alphanumeric.take(10).mkString
}
