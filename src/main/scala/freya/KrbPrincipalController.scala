package freya

import cats.effect.Sync
import com.goyeau.kubernetes.client.crd.CustomResource
import freya.Controller.NewStatus

object KrbPrincipalController {

  implicit def instance[F[_]: Sync]: Controller[F, Principals, PrincipalsStatus] =
    new Controller[F, Principals, PrincipalsStatus]() {
      override def onAdd(
          resource: CustomResource[Principals, PrincipalsStatus]
      ): F[NewStatus[PrincipalsStatus]] = super.onAdd(resource)

      override def onModify(
          resource: CustomResource[Principals, PrincipalsStatus]
      ): F[NewStatus[PrincipalsStatus]] = super.onModify(resource)

      override def onDelete(
          resource: CustomResource[Principals, PrincipalsStatus]
      ): F[Unit] = super.onDelete(resource)
    }

}
