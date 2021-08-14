package freya

import cats.effect._
import cats.implicits.{catsSyntaxFlatMapOps, toFunctorOps}
import com.goyeau.kubernetes.client.EventType.{ADDED, DELETED, ERROR, MODIFIED}
import com.goyeau.kubernetes.client._
import com.goyeau.kubernetes.client.crd.{CrdContext, CustomResource}
import freya.Controller.NewStatus
import fs2.{Pipe, Stream}
import io.circe.generic.extras.auto._
import io.circe.{Decoder, Encoder}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.io.File

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
    val serverStream = watchCr[IO, KrbServer, KrbServerStatus](kubernetesClient)
    val principalsStream =
      watchCr[IO, Principals, PrincipalsStatus](kubernetesClient)

    IO.race(serverStream, principalsStream).map(_.merge)
  }

  def watchCr[F[_]: Sync, A: Encoder: Decoder, B: Encoder: Decoder](
      kubernetesClient: Resource[F, KubernetesClient[F]]
  )(implicit
      controller: Controller[F, A, B]
  ) = {
    val kind = classOf[Principals].getSimpleName
    val plural = s"${kind.toLowerCase}s"

    kubernetesClient
      .use { client =>
        val context = CrdContext(group, "v1", plural)
        client
          .customResources[A, B](context)
          .watch()
          .through(handler[F, A, B])
          .compile
          .drain
      }
      .as(ExitCode.Success)
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
