package krboperator

import cats.effect.Sync
import cats.syntax.all.catsSyntaxApply
import com.goyeau.kubernetes.client.crd.CustomResource
import krboperator.Controller.{NewStatus, noStatus}
import org.typelevel.log4cats.Logger

object KrbServerController {

  implicit def instance[F[_]: Logger](implicit
      F: Sync[F]
  ): Controller[F, KrbServer, KrbServerStatus] =
    new Controller[F, KrbServer, KrbServerStatus]() {
      override def onAdd(
          resource: CustomResource[KrbServer, KrbServerStatus]
      ): F[NewStatus[KrbServerStatus]] =
        Logger[F]
          .info(s"received on add: $resource") *> noStatus[F, KrbServerStatus]

      override def onModify(
          resource: CustomResource[KrbServer, KrbServerStatus]
      ): F[NewStatus[KrbServerStatus]] = super.onModify(resource)

      override def onDelete(
          resource: CustomResource[KrbServer, KrbServerStatus]
      ): F[Unit] = super.onDelete(resource)
    }

}
