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
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.io.File
import scala.reflect.ClassTag

object TestApp extends IOApp with Codecs {
  implicit def unsafeLogger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  val kubernetesClient: Resource[IO, KubernetesClient[IO]] =
    KubernetesClient[IO](
      KubeConfig.fromFile[IO](
        new File(s"${System.getProperty("user.home")}/.kube/config")
      )
    )

  val group = "krb-operator.novakov-alexey.github.io"

  override def run(args: List[String]): IO[ExitCode] = {
    implicit lazy val serverController = KrbServerController.instance[IO]
    implicit lazy val principalController = KrbPrincipalController.instance[IO]

    kubernetesClient
      .use { client =>
        val serverStream = watchCr[IO, KrbServer, KrbServerStatus](client)
        val principalsStream =
          watchCr[IO, Principals, PrincipalsStatus](client)

        Stream(serverStream, principalsStream).parJoinUnbounded.compile.drain
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
    val context = CrdContext(group, "v1", plural)
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
