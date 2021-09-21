package krboperator

import cats.effect.Sync
import cats.syntax.all._
import com.goyeau.kubernetes.client.crd.CustomResource
import krboperator.Controller.{NewStatus, NoStatus, noStatus}

import scala.language.implicitConversions

object Controller {
  type NewStatus[U] = Option[U]
  type NoStatus = NewStatus[Unit]

  def noStatus[F[_], U](implicit F: Sync[F]): F[Option[U]] = F.pure(none[U])
}

abstract class Controller[F[_], T, U](implicit val F: Sync[F]) {

  implicit def unitToNoStatus(unit: F[Unit]): F[NoStatus] =
    unit *> noStatus

  def onAdd(resource: CustomResource[T, U]): F[NewStatus[U]] = noStatus

  def onModify(resource: CustomResource[T, U]): F[NewStatus[U]] = noStatus

  def reconcile(resource: CustomResource[T, U]): F[NewStatus[U]] = noStatus

  def onError(resource: CustomResource[T, U]): F[NewStatus[U]] = noStatus

  def onDelete(resource: CustomResource[T, U]): F[Unit] = F.unit
}
