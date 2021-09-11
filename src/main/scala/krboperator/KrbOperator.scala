package krboperator

import cats.effect._
import cats.implicits.{catsSyntaxFlatMapOps, toBifunctorOps, toFunctorOps}
import com.goyeau.kubernetes.client.EventType.{ADDED, DELETED, ERROR, MODIFIED}
import com.goyeau.kubernetes.client._
import com.goyeau.kubernetes.client.crd.{CrdContext, CustomResource}
import fs2.{Pipe, Stream}
import io.circe.generic.extras.auto._
import io.circe.{Decoder, Encoder}
import krboperator.Controller.NewStatus
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

  val group =
    "krb-operator.novakov-alexey.github.io" //TODO: extract to configuration

  def crdContext[A: ClassTag] = {
    val kind = implicitly[ClassTag[A]].runtimeClass.getSimpleName
    val plural = s"${kind.toLowerCase}s"
    CrdContext(
      group,
      "v1",
      plural
    ) //TODO: version to extract to configuration
  }

  override def run(args: List[String]): IO[ExitCode] =
    kubernetesClient
      .use { client =>
        for {
          operatorCfg <- IO.fromEither(
            AppConfig.load
              .leftMap(e => new RuntimeException(s"failed to load config: $e"))
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
        crdContext[KrbServer]
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

  def watchCr[F[
      _
  ]: Sync, Resource: Encoder: Decoder: ClassTag, Status: Encoder: Decoder](
      kubernetesClient: KubernetesClient[F]
  )(implicit
      controller: Controller[F, Resource, Status]
  ) = {
    val context = crdContext[Resource]
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
  ]], NewStatus[Status]] =
    _.flatMap { s =>
      val action = s match {
        case Left(err) =>
          Logger[F].error(s"Error on watching events: $err").map(_ => None)
        case Right(event) =>
          Logger[F].debug(s"Received event: $event") >> {
            event.`type` match { //TODO: do not crash on controller IO error, but just log as error
              case ADDED    => controller.onAdd(event.`object`)
              case MODIFIED => controller.onModify(event.`object`)
              case DELETED =>
                controller.onDelete(event.`object`).map(_ => None)
              case ERROR => controller.onError(event.`object`)
            }
          }
      }
      Stream.eval(action)
    }
}
