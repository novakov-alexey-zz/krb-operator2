package krboperator

import cats.effect._
import cats.implicits.{catsSyntaxFlatMapOps, toFunctorOps}
import com.goyeau.kubernetes.client.EventType.{ADDED, DELETED, ERROR, MODIFIED}
import com.goyeau.kubernetes.client._
import com.goyeau.kubernetes.client.crd.{CrdContext, CustomResource}
import fs2.{Pipe, Stream}
import io.circe.generic.extras.auto._
import io.circe.{Decoder, Encoder}
import krboperator.Controller.NewStatus
import krboperator.service.{Kadmin, Secrets}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.io.File
import scala.reflect.ClassTag

object KrbOperator extends IOApp with Codecs {
  implicit def unsafeLogger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  val kubernetesClient: Resource[IO, KubernetesClient[IO]] =
    KubernetesClient[IO](
      KubeConfig.fromFile[IO](
        new File(s"${System.getProperty("user.home")}/.kube/config")
      )
    )

  val group =
    "krb-operator.novakov-alexey.github.io" //TODO: extract to configuration

  val operatorCfg =
    AppConfig.load.fold(e => sys.error(s"failed to load config: $e"), identity)

  override def run(args: List[String]): IO[ExitCode] = {
    implicit lazy val serverController = ServerController.instance[IO]

    kubernetesClient
      .use { client =>
        val secrets = new Secrets[IO](client, operatorCfg)
        val kadmin = new Kadmin[IO](client, operatorCfg)
        implicit lazy val principalController =
          new PrincipalController[IO](secrets, kadmin, client)

        val server = watchCr[IO, KrbServer, KrbServerStatus](client)
        val principals = watchCr[IO, Principals, PrincipalsStatus](client)
        Stream(server, principals).parJoinUnbounded.compile.drain
      }
      .as(ExitCode.Success)
  }

  def watchCr[F[
      _
  ]: Sync, Resource: Encoder: Decoder: ClassTag, Status: Encoder: Decoder](
      kubernetesClient: KubernetesClient[F]
  )(implicit
      controller: Controller[F, Resource, Status]
  ) = {
    val kind = implicitly[ClassTag[Resource]].runtimeClass.getSimpleName
    val plural = s"${kind.toLowerCase}s"
    val context = CrdContext(
      group,
      "v1",
      plural
    ) //TODO: version to extract to configuration
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
            event.`type` match {
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
