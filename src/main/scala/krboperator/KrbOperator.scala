package krboperator

import cats.effect._
import cats.implicits._
import com.goyeau.kubernetes.client.EventType.{ADDED, DELETED, ERROR, MODIFIED}
import com.goyeau.kubernetes.client._
import com.goyeau.kubernetes.client.crd.{CrdContext, CustomResource}
import fs2.{Pipe, Stream}
import io.circe.generic.extras.auto._
import io.circe.{Decoder, Encoder}
import io.k8s.apiextensionsapiserver.pkg.apis.apiextensions.v1.CustomResourceDefinition
import krboperator.service.Template.implicits._
import krboperator.service.{Kadmin, Secrets, Template}
import krboperator.Controller.{NewStatus, noStatus}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.io.File
import scala.reflect.ClassTag

object KrbOperator extends IOApp with Codecs {
  implicit def unsafeLogger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  val kubernetesClient =
    KubernetesClient[IO](
      KubeConfig.fromFile[IO](
        new File(s"${System.getProperty("user.home")}/.kube/config")
      )
    )

  override def run(args: List[String]): IO[ExitCode] =
    kubernetesClient
      .use { implicit client =>
        for {
          operatorCfg <- IO.fromEither(
            AppConfig.load
              .leftMap(e => new RuntimeException(s"failed to load config: $e"))
          )
          serverCtx = crds.context[KrbServer]
          _ <- createCrdIfAbsent[IO, KrbServer](
            serverCtx,
            crds.server.definition(serverCtx)
          )
          principalCtx = crds.context[Principals]
          _ <- createCrdIfAbsent[IO, Principals](
            principalCtx,
            crds.principal.definition(principalCtx)
          )
          _ <- createWatchers(
            operatorCfg,
            serverCtx,
            principalCtx
          ).parJoinUnbounded.compile.drain
        } yield ()
      }
      .as(ExitCode.Success)

  def createWatchers(
      operatorCfg: KrbOperatorCfg,
      serverCtx: CrdContext,
      principalCtx: CrdContext
  )(implicit client: KubernetesClient[IO]) = {
    val secrets = new Secrets(client, operatorCfg)
    val kadmin = new Kadmin(client, operatorCfg)
    val template = new Template(client, secrets, operatorCfg)

    implicit lazy val serverController =
      new ServerController(
        template,
        secrets,
        client,
        crds.context[KrbServer]
      )
    implicit lazy val principalController =
      new PrincipalController(
        secrets,
        kadmin,
        client,
        serverController,
        operatorCfg
      )

    val server = watchCr[IO, KrbServer, KrbServerStatus](serverCtx)
    val principals = watchCr[IO, Principals, PrincipalsStatus](principalCtx)
    Stream(server, principals)
  }

  def createCrdIfAbsent[F[_]: Sync, Resource](
      ctx: CrdContext,
      customDefinition: CustomResourceDefinition,
      attempts: Int = 1
  )(implicit client: KubernetesClient[F]): F[Unit] = {
    val crdName = crds.crdName(ctx)
    for {
      found <- client.customResourceDefinitions
        .get(crdName)
        .map(_ => true)
        .recoverWith { case _ =>
          false.pure[F]
        }
      _ <-
        if (!found && attempts > 0)
          client.customResourceDefinitions.create(
            customDefinition
          ) *> createCrdIfAbsent(ctx, customDefinition, attempts - 1)
        else if (!found)
          Sync[F].raiseError(new RuntimeException(s"CRD '$crdName' not found"))
        else Sync[F].unit
    } yield ()
  }

  def watchCr[F[
      _
  ]: Sync, Resource: Encoder: Decoder: ClassTag, Status: Encoder: Decoder](
      ctx: CrdContext
  )(implicit
      controller: Controller[F, Resource, Status],
      kubernetesClient: KubernetesClient[F]
  ) = {
    val context = crds.context[Resource]
    Stream.eval(
      Logger[F].info(s"Watching context: $context")
    ) >> kubernetesClient
      .customResources[Resource, Status](context)
      .watch()
      .through(handler[F, Resource, Status])
      .through(submitStatus[F, Resource, Status](ctx))
  }

  def handler[F[_]: Sync, Resource, Status](implicit
      controller: Controller[F, Resource, Status]
  ): Pipe[F, Either[String, WatchEvent[
    CustomResource[Resource, Status]
  ]], Option[CustomResource[Resource, Status]]] =
    _.flatMap { s =>
      val action = s match {
        case Left(err) =>
          Logger[F].error(s"Error on watching events: $err") *> Sync[F].pure(
            none
          )
        case Right(event) =>
          Logger[F].debug(s"Received event: $event") >> {
            val status =
              event.`type` match {
                case ADDED =>
                  controller
                    .onAdd(event.`object`)
                    .map(addStatus(_, event.`object`))
                case MODIFIED =>
                  controller
                    .onModify(event.`object`)
                    .map(addStatus(_, event.`object`))
                case DELETED =>
                  controller.onDelete(event.`object`).map(_ => none)
                case ERROR =>
                  controller
                    .onError(event.`object`)
                    .map(addStatus(_, event.`object`))
              }

            status.handleErrorWith { e =>
              Logger[F].error(e)(
                s"Controller failed to handle event type ${event.`type`}"
              ) *> Sync[F].pure(none)
            }
          }
      }
      Stream.eval(action)
    }

  def addStatus[Resource, Status](
      status: NewStatus[Status],
      cr: CustomResource[Resource, Status]
  ): Option[CustomResource[Resource, Status]] =
    status match {
      case Some(_) => cr.copy(status = status).some
      case None    => none
    }

  def submitStatus[F[
      _
  ]: Sync, Resource: Encoder: Decoder, Status: Encoder: Decoder](
      ctx: CrdContext
  )(implicit
      client: KubernetesClient[F]
  ): Pipe[F, Option[CustomResource[Resource, Status]], Unit] =
    _.flatMap { cr =>
      val action = cr match {
        case Some(r) =>
          for {
            name <- Sync[F].fromOption(
              r.metadata.flatMap(_.name),
              new RuntimeException(s"Resource name is empty: $r")
            )
            namespace <- Sync[F].fromOption(
              r.metadata.flatMap(_.namespace),
              new RuntimeException(s"Resource namespace is empty: $r")
            )
            _ <- client
              .customResources[Resource, Status](ctx)
              .namespace(namespace)
              .updateStatus(name, r)
              .flatMap(status =>
                if (status.isSuccess) Sync[F].unit
                else
                  Logger[F].error(new RuntimeException(status.toString()))(
                    "Status updated failed on Kubernetes side"
                  )
              )
          } yield ()
        case None => Sync[F].unit
      }

      Stream.eval(action.handleErrorWith { e =>
        Logger[F].error(e)(
          s"Failed to submit status"
        ) *> Sync[F].pure(none)
      })
    }
}
