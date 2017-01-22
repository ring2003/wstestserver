import java.io.File
import java.nio.charset.Charset
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitOption, Files, Path, Paths}
import java.util.function.{BiPredicate, Predicate}

import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props, Terminated}
import akka.event.LoggingAdapter
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.ws.TextMessage.Strict
import akka.http.scaladsl.model.ws.{TextMessage, UpgradeToWebSocket}
import akka.http.scaladsl.model._
import akka.stream._
import akka.stream.scaladsl._

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.sys.process._
/**
  * Created by codemonkey on 1/12/2017.
  */

case class Launch(casename: String, buildId: String, releaseId: String)
case class CommandOutput(payload: String)
case object EndOfCommand

class TestifyServer extends Actor {
  import context.{dispatcher, _}

  val log: LoggingAdapter = system.log
  implicit val mat: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(system).withInputBuffer(1024, 4096))(context)
  val commandTestify2: Seq[String] = Seq("/tmp/test/run")

  def receive: Receive = {
    case x: Launch => {
      val to = context.actorSelection("../server/StreamSupervisor*/*")
      val f = Future { scala.concurrent.blocking { Process(commandTestify2 :+ x.casename).lineStream_!.foreach(to ! _) } }
      Await.result(f, Duration.Inf)
    }
    case _ => log.warning("Dropping invalid request.")
  }
}

class Testify2Frontend extends Actor {
  import akka.http.scaladsl.Http
  import context._

  implicit val mat: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(system))(context)
  mat.settings.withInputBuffer(1024, 4096)

  var sessions: Set[ActorRef] = Set.empty

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    context.children.foreach(_ ! PoisonPill)
    super.preRestart(reason, message)
  }

  def handler: Source[Strict, Unit] =
    Source
      .actorRef(4, OverflowStrategy.dropHead)
      .mapMaterializedValue(r => {
        context.watch(r)
        sessions += r
      })
      .map( (s: String) => TextMessage(s))
      .buffer(4096, OverflowStrategy.backpressure)

  val requestHandler: HttpRequest => HttpResponse  = {
    case req @ HttpRequest(GET, Uri.Path("/show"), seqHeader, reqEnt, proto) => {
      req.header[UpgradeToWebSocket] match {
        case Some(x) => x.handleMessagesWithSinkSource(Sink.ignore, handler)
        case _ => HttpResponse(404, entity = "no")
      }
    }
    case req @ HttpRequest(GET, Uri.Path("/log"), seqHeader, reqEnt, p) => {
      val query = req.uri.query(Charset.defaultCharset(), Uri.ParsingMode.Strict)
      if  (query.nonEmpty) {
        val rundir = query.get("rundir")
        val casename = query.get("casename")
        if (rundir.nonEmpty && casename.nonEmpty) {
          val rundirPath = Paths.get(rundir.get)
          val results: mutable.Queue[Path] = mutable.Queue()
          Files.find(rundirPath, 1, (t: Path, u: BasicFileAttributes) => t.toString.matches("${casename}_2017*"), FileVisitOption.FOLLOW_LINKS).forEach( (x) => results.enqueue(x))
          val latest = results.toStream.sortBy(x => -Files.getLastModifiedTime(x).toMillis).headOption
          if (latest.nonEmpty) {
            val testLog = latest.get.resolve("test.log")
            if (Files.exists(testLog)) HttpResponse(200, entity = HttpEntity.fromPath(ContentTypes.`text/plain(UTF-8)`, testLog))
            else HttpResponse(200, entity = "What the hell?")
          }
          else HttpResponse(200, entity = "There is no such case under rundir.")
        } else HttpResponse(200, entity = "Bad usage!")
      } else HttpResponse(200, entity = "No way!")
    }
    case req @ HttpRequest(GET, Uri.Path("/metrics"), seqHeader, reqEnt, p) => HttpResponse(200, entity = s"${sessions.size}")
    case req @ HttpRequest(GET, Uri.Path("/run"), seqHeader, reqEnt, p) => {
      val uriQuery = req.uri.query(Charset.defaultCharset(), Uri.ParsingMode.Strict)
      if (uriQuery.nonEmpty) {
        val casename = uriQuery.get("casename")
        val release = uriQuery.get("release")
        val build = uriQuery.get("build")
        if (casename.nonEmpty && release.nonEmpty && build.nonEmpty) {
          val launcher = context.actorSelection("../launcher")
          launcher ! Launch(casename=casename.get, buildId = build.get, releaseId = release.get)
          HttpResponse(200, entity=s"casename=${casename.get}, buildId = ${build.get}, releaseId = ${release.get}")
        }
        else HttpResponse(404, entity="Bad usage!Need give (casename,release,build)!")
      }
      else HttpResponse(404, entity="Bad usage!Need give (casename,release,build)!")
    }
    case req: HttpRequest => {
      req.discardEntityBytes()
      HttpResponse(404, entity="unknown")
    }
  }

  Http().bindAndHandleSync(requestHandler, interface = "0.0.0.0", port=4044)

  def receive: PartialFunction[Any, Unit] = {
    case x:CommandOutput=> sessions.foreach(_ ! x)
    case y:EndOfCommand.type => sessions.foreach(context.stop)
    case z: Terminated => {
      val death = z.getActor
      context.unwatch(death)
      println(s"Remove death: ${death.path.name}")
      sessions -= death
    }
    case _ => system.log.info("drop")
  }
}

object Main extends App {

  val system = ActorSystem("system")
  val server = system.actorOf(Props(classOf[Testify2Frontend]), "server")
  system.actorOf(Props(classOf[TestifyServer]), "launcher")
}