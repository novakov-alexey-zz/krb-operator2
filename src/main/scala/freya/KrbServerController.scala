package freya

import cats.effect.Sync
import com.goyeau.kubernetes.client.crd.CustomResource
import freya.Controller.NewStatus

object KrbServerController {

  implicit def instance[F[_]: Sync]
      : Controller[F, KrbServer, KrbServerStatus] =
    new Controller[F, KrbServer, KrbServerStatus]() {
      override def onAdd(
          resource: CustomResource[KrbServer, KrbServerStatus]
      ): F[NewStatus[KrbServerStatus]] = super.onAdd(resource)

      override def onModify(
          resource: CustomResource[KrbServer, KrbServerStatus]
      ): F[NewStatus[KrbServerStatus]] = super.onModify(resource)

      override def onDelete(
          resource: CustomResource[KrbServer, KrbServerStatus]
      ): F[Unit] = super.onDelete(resource)
    }

}
