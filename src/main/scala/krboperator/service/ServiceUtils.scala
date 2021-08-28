package krboperator.service

import cats.effect.Sync
import io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta

object ServiceUtils {

  def getNamespace[F[_]: Sync](meta: ObjectMeta): F[String] =
    Sync[F].fromOption(
      meta.namespace,
      new RuntimeException(s"Namespace is empty in $meta")
    )
}
