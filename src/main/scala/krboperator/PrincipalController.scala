package krboperator

import cats.effect.Sync
import cats.implicits._
import com.goyeau.kubernetes.client.crd.CustomResource
import org.typelevel.log4cats.Logger
import io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta
import krboperator.Controller.{NewStatus, noStatus}
import krboperator.service.{Kadmin, KadminContext, KerberosState, Secrets}
import krboperator.LoggingUtils._
import PrincipalController._

import java.nio.file.Path
import com.goyeau.kubernetes.client.KubernetesClient
import cats.Parallel

object PrincipalController {
  val ServerLabel = "krb-operator.novakov-alexey.github.io/server"
}

class PrincipalController[F[_]: Parallel](
    secret: Secrets[F],
    kadmin: Kadmin[F],
    client: KubernetesClient[F],
    parallelSecret: Boolean = true
)(implicit F: Sync[F], val logger: Logger[F])
    extends Controller[F, Principals, PrincipalsStatus]
    with LoggingUtils[F] {

  override def onAdd(
      resource: CustomResource[Principals, PrincipalsStatus]
  ): F[NewStatus[PrincipalsStatus]] =
    getMetadata(resource.metadata).flatMap(m => onApply(resource.spec, m))

  override def onModify(
      resource: CustomResource[Principals, PrincipalsStatus]
  ): F[NewStatus[PrincipalsStatus]] =
    getMetadata(resource.metadata).flatMap(onApply(resource.spec, _))

  override def onDelete(
      resource: CustomResource[Principals, PrincipalsStatus]
  ): F[Unit] = super.onDelete(resource)

  private def getMetadata(meta: Option[ObjectMeta]): F[ObjectMeta] =
    F.fromOption(meta, new RuntimeException("Metadata is empty"))

  private def onApply(
      principals: Principals,
      meta: ObjectMeta
  ): F[NewStatus[PrincipalsStatus]] =
    for {
      server <- getKrbServer(meta)
      missingSecrets <- secret.findMissing(
        meta,
        principals.list.map(_.secret.name).toSet
      )
      created <- missingSecrets.toList match {
        case Nil =>
          debug(
            meta.namespace.get,
            s"There are no missing secrets"
          ) *> F.pure(List.empty[Unit])
        case _ =>
          info(
            meta.namespace.get,
            s" There are ${missingSecrets.size} missing secrets, name(s): $missingSecrets"
          ) *> createSecrets(server, principals, meta, missingSecrets)
      }
      _ <- F.whenA(created.nonEmpty)(
        info(meta.namespace.get, s"${created.length} secrets created")
      )
    } yield PrincipalsStatus(
      processed = true,
      created.length,
      principals.list.length
    ).some

  private def currentServers
      : F[List[CustomResource[KrbServer, KrbServerStatus]]] = ??? //TODO

  private[krboperator] def getKrbServer(
      meta: ObjectMeta
  ): F[CustomResource[KrbServer, KrbServerStatus]] = for {
    serverName <- F.fromEither(
      meta.labels
        .getOrElse(Map.empty)
        .collectFirst { case (ServerLabel, v) => v }
        .toRight(
          new RuntimeException(
            s"Current resource does not have a label '$ServerLabel'"
          )
        )
    )
    servers <- currentServers
    serverOrError = servers
      .find { r =>
        r.metadata.exists(_.name.exists(_ == serverName))
      }
      .map(_.asRight[Throwable])
      .getOrElse(
        Either.left(
          new RuntimeException(
            s"Failed to find ${classOf[KrbServer].getSimpleName()} resource with name '$serverName'"
          )
        )
      )
    server <- F.fromEither(serverOrError)
  } yield server

  private def createSecrets(
      server: CustomResource[KrbServer, KrbServerStatus],
      principals: Principals,
      meta: ObjectMeta,
      missingSecrets: Set[String]
  ): F[List[Unit]] =
    for {
      pwd <- secret.getAdminPwd(meta)
      context = KadminContext(
        server.metadata.get.name.get,
        server.spec.realm,
        meta,
        pwd
      )
      created <- {
        lazy val ns = meta.namespace.get
        val tasks = missingSecrets
          .map(s => (s, principals.list.filter(_.secret.name == s)))
          .map { case (secretName, principals) =>
            for {
              _ <- debug(
                ns,
                s"Creating secret: $secretName"
              )
              state <- kadmin.createPrincipalsAndKeytabs(principals, context)
              statuses <- copyKeytabs(ns, state)
              _ <- checkStatuses(statuses)
              _ <- secret.create(ns, state.principals, secretName)
              _ <- info(
                ns,
                s"$checkMark Keytab secret '$secretName' created"
              )
              _ <- removeWorkingDirs(ns, state)
                .handleErrorWith { e =>
                  error(
                    ns,
                    s"Failed to delete working directory(s) with keytab(s) in POD $ns/${state.podName}",
                    e
                  )
                }
            } yield ()
          }
          .toList
        if (parallelSecret) tasks.parSequence else tasks.sequence
      }
    } yield created

  private def checkStatuses(statuses: List[(Path, Boolean)]): F[Unit] = {
    val notAllCopied = !statuses.forall { case (_, copied) => copied }
    F.whenA(notAllCopied)(F.raiseError[Unit] {
      val paths = statuses
        .filter { case (_, copied) =>
          !copied
        }
        .map { case (path, _) => path }
      new RuntimeException(s"Failed to upload keytab(s) $paths into POD")
    })
  }

  private def copyKeytabs(
      namespace: String,
      state: KerberosState
  ): F[List[(Path, Boolean)]] =
    F.delay(state.principals.foldLeft(List.empty[(Path, Boolean)]) {
      case (acc, principals) =>
        val path = principals.keytabMeta.path
        debug(
          namespace,
          s"Copying keytab '$path' from $namespace/${state.podName} POD"
        )
        val copied = true //TODO
        // client.pods
        // .namespace(namespace)
        // .get(state.podName)
        // .inContainer(operatorCfg.kadminContainer)
        // .file(path.toString)
        // .copy(path)

        acc :+ (path, copied)
    })

  private def removeWorkingDirs(
      namespace: String,
      state: KerberosState
  ): F[Unit] =
    state.principals
      .map { p =>
        kadmin.removeWorkingDir(namespace, state.podName, p.keytabMeta.path)
      }
      .sequence
      .void
}
