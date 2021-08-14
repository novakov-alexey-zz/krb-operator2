package krboperator

import cats.effect.Sync
import cats.syntax.all.catsSyntaxApply
import com.goyeau.kubernetes.client.crd.CustomResource
import krboperator.Controller.{NewStatus, noStatus}
import org.typelevel.log4cats.Logger

object KrbPrincipalController {

  implicit def instance[F[_]: Logger](implicit
      F: Sync[F]
  ): Controller[F, Principals, PrincipalsStatus] =
    new Controller[F, Principals, PrincipalsStatus]() {
      override def onAdd(
          resource: CustomResource[Principals, PrincipalsStatus]
      ): F[NewStatus[PrincipalsStatus]] = Logger[F]
        .info(s"received on add: $resource") *> noStatus[F, PrincipalsStatus]

      override def onModify(
          resource: CustomResource[Principals, PrincipalsStatus]
      ): F[NewStatus[PrincipalsStatus]] = super.onModify(resource)

      override def onDelete(
          resource: CustomResource[Principals, PrincipalsStatus]
      ): F[Unit] = super.onDelete(resource)
    }

}
