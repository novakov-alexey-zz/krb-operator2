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
      .use { client =>
        for {
          operatorCfg <- IO.fromEither(
            AppConfig.load
              .leftMap(e => new RuntimeException(s"failed to load config: $e"))
          )
          serverCtx = crds.context[KrbServer]
          _ <- createCrdIfAbsent(
            client,
            serverCtx,
            crds.server.definition(serverCtx)
          )
          principalCtx = crds.context[Principals]
          _ <- createCrdIfAbsent(
            client,
            principalCtx,
            crds.principal.definition(principalCtx)
          )
          _ <- createWatchers(
            client,
            operatorCfg
          ).parJoinUnbounded.compile.drain
        } yield ()
      }
      .as(ExitCode.Success)

  def createWatchers(
      client: KubernetesClient[IO],
      operatorCfg: KrbOperatorCfg
  ) = {
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

    val server = watchCr[IO, KrbServer, KrbServerStatus](client)
    val principals = watchCr[IO, Principals, PrincipalsStatus](client)
    Stream(server, principals)
  }

  def createCrdIfAbsent[F[_]: Async, Resource](
      client: KubernetesClient[F],
      ctx: CrdContext,
      customDefinition: CustomResourceDefinition,
      attempts: Int = 1
  ): F[Unit] = {
    val crdName = crds.crdName(ctx)
    for {
      found <- client.customResourceDefinitions
        .get(crdName)
        .map(_ => true)
        .recoverWith { case _ =>
          Async[F].pure(false)
        }
      _ <-
        if (!found && attempts > 0)
          client.customResourceDefinitions.create(
            customDefinition
          ) *> createCrdIfAbsent(client, ctx, customDefinition, attempts - 1)
        else if (!found)
          Sync[F].raiseError(new RuntimeException(s"CRD '$crdName' not found"))
        else Sync[F].unit
    } yield ()
  }

  def watchCr[F[
      _
  ]: Sync, Resource: Encoder: Decoder: ClassTag, Status: Encoder: Decoder](
      kubernetesClient: KubernetesClient[F]
  )(implicit
      controller: Controller[F, Resource, Status]
  ) = {
    val context = crds.context[Resource]
    Stream.eval(
      Logger[F].info(s"Watching context: $context")
    ) >> kubernetesClient
      .customResources[Resource, Status](context)
      .watch()
      .through(handler[F, Resource, Status])
  }

  def handler[F[_]: Sync, Resource, Status](implicit
      controller: Controller[F, Resource, Status]
  ): Pipe[F, Either[String, WatchEvent[
    CustomResource[Resource, Status]
  ]], Unit] =
    _.flatMap { s =>
      val action = s match {
        case Left(err) =>
          Logger[F].error(s"Error on watching events: $err").void
        case Right(event) =>
          Logger[F].debug(s"Received event: $event") >> {
            val status =
              event.`type` match {
                case ADDED    => controller.onAdd(event.`object`)
                case MODIFIED => controller.onModify(event.`object`)
                case DELETED =>
                  controller.onDelete(event.`object`).map(_ => None)
                case ERROR => controller.onError(event.`object`)
              }

            //TODO: submit status
            status.void.handleErrorWith(e =>
              Logger[F].error(e)(
                s"Controller failed to handle event type ${event.`type`}"
              )
            )
          }
      }
      Stream.eval(action)
    }
}
