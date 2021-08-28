package krboperator

import cats.effect.Sync
import cats.implicits._
import cats.syntax.all.catsSyntaxApply
import com.goyeau.kubernetes.client.KubernetesClient
import com.goyeau.kubernetes.client.crd.{
  CrdContext,
  CustomResource,
  CustomResourceList
}
import io.circe.generic.extras.auto._
import io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta
import krboperator.Controller.NewStatus
import krboperator.LoggingUtils._
import krboperator.service.{Secrets, Template}
import org.typelevel.log4cats.Logger

class ServerController[F[_]](
    template: Template[F],
    secret: Secrets[F],
    client: KubernetesClient[F],
    crdContext: CrdContext
)(implicit
    F: Sync[F],
    val logger: Logger[F]
) extends Controller[F, KrbServer, KrbServerStatus]
    with LoggingUtils[F]
    with Codecs {

  override def onAdd(
      resource: CustomResource[KrbServer, KrbServerStatus]
  ): F[NewStatus[KrbServerStatus]] =
    onApply("add", resource)

  override def onModify(
      resource: CustomResource[KrbServer, KrbServerStatus]
  ): F[NewStatus[KrbServerStatus]] =
    onApply("modify", resource)

  override def reconcile(
      resource: CustomResource[KrbServer, KrbServerStatus]
  ): F[NewStatus[KrbServerStatus]] =
    onApply("reconcile", resource)

  override def onDelete(
      resource: CustomResource[KrbServer, KrbServerStatus]
  ): F[Unit] = for {
    (meta, ns) <- getNamespace(resource.metadata)
    _ <- info(ns, s"delete event: ${resource.spec}, $meta")
    status <- template.delete(meta)
  } yield status

  def currentServers: F[CustomResourceList[KrbServer, KrbServerStatus]] =
    client.customResources[KrbServer, KrbServerStatus](crdContext).list()

  private def getNamespace(meta: Option[ObjectMeta]): F[(ObjectMeta, String)] =
    for {
      meta <- F.fromOption(meta, new RuntimeException("Metadata is empty"))
      ns <- F.fromOption(
        meta.namespace,
        new RuntimeException(s"Namespace is empty in $meta")
      )

    } yield (meta, ns)

  private def onApply(
      eventName: String,
      resource: CustomResource[KrbServer, KrbServerStatus]
  ): F[NewStatus[KrbServerStatus]] = {
    for {
      (meta, ns) <- getNamespace(resource.metadata)
      _ <- info(
        ns,
        s"'$eventName' event: ${resource.spec}, ${resource.metadata}"
      )
      service <- template.findService(meta)
      _ <- service match {
        case Some(_) =>
          debug(
            ns,
            s"$checkMark [${meta.name}] Service is found, so skipping its creation"
          )

        case None =>
          template.createService(meta) *>
            info(ns, s"$checkMark Service ${meta.name} created")
      }
      adminSecret <- secret.findAdminSecret(meta)
      _ <- adminSecret match {
        case Some(_) =>
          debug(
            ns,
            s"$checkMark [${meta.name}] Admin Secret is found, so skipping its creation"
          )

        case None =>
          secret.createAdminSecret(meta) *>
            info(
              ns,
              s"$checkMark Admin secret ${meta.name} created"
            )
      }
      deployment <- template.findDeployment(meta)
      _ <- deployment match {
        case Some(_) =>
          debug(
            ns,
            s"$checkMark [${meta.name}] Deployment is found, so skipping its creation"
          )

        case None =>
          for {
            _ <- template.createDeployment(meta, resource.spec.realm)
            _ <- template.waitForDeployment(meta)
            _ <-
              info(
                ns,
                s"$checkMark deployment ${meta.name} created"
              )
          } yield ()
      }
    } yield KrbServerStatus(processed = true).some
  }.handleErrorWith { e =>
    val ns = resource.metadata.flatMap(_.namespace).getOrElse("n/a")
    error(
      ns,
      s"[$ns] Failed to handle create/apply event: $resource, ${resource.metadata}",
      e
    ) *> F.pure(Some(KrbServerStatus(processed = false, e.getMessage)))
  }
}
