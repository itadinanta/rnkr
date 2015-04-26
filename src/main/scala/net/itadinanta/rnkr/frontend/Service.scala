package net.itadinanta.rnkr.frontend

import akka.actor.Actor
import spray.routing._
import spray.http._
import MediaTypes._
import spray.json.DefaultJsonProtocol
import scala.concurrent.ExecutionContext
import net.itadinanta.rnkr.engine.manager.Manager
import akka.actor.ActorContext
import spray.httpx.SprayJsonSupport
import spray.httpx.marshalling.MetaMarshallers
import scala.concurrent.Future
import net.itadinanta.rnkr.backend.Cassandra
import net.itadinanta.rnkr.core.tree.Row
import net.itadinanta.rnkr.core.tree.RankedTreeMap
import net.itadinanta.rnkr.engine.leaderboard.LeaderboardBuffer
import net.itadinanta.rnkr.engine.leaderboard.UpdateMode._
import net.itadinanta.rnkr.engine.leaderboard.Post
import net.itadinanta.rnkr.engine.leaderboard.Update
import net.itadinanta.rnkr.engine.leaderboard.Entry
import net.itadinanta.rnkr.engine.leaderboard.Attachments
import spray.json.JsonFormat
import spray.json.JsString
import spray.json.JsValue
import scalaz.ImmutableArray
import spray.json.DeserializationException
import akka.actor.Props

trait Service extends HttpService with SprayJsonSupport with DefaultJsonProtocol {
	val cassandra: Cassandra
	implicit val executionContext: ExecutionContext
	implicit object AttachmentJsonFormat extends JsonFormat[Attachments] {
		def write(c: Attachments) =
			JsString(new String(c.data.toArray, "UTF8"))

		def read(value: JsValue) = value match {
			case JsString(s) => Attachments(s)
			case _ => throw new DeserializationException("Invalid format for Attachments")
		}
	}
	implicit val jsonEntry = jsonFormat5(Entry)

	val manager = new Manager(cassandra, () => LeaderboardBuffer())

	val rnkrRoute = pathPrefix("rnkr" / "leaderboard" / """[a-zA-Z0-9]+""".r) { treeId =>
		val lb = manager.get(treeId)
		pathEnd {
			(post | put) {
				formFields('score, 'entrant, 'attachments ?, 'force ? false) { (score, entrant, attachments, force) =>
					complete(lb.flatMap(_.post(Post(score.toLong, entrant, Attachments(attachments)), if (force) LastWins else BestWins)).map(_.newEntry))
				}
			}
		} ~ path("around") {
			parameter('entrant, 'count ? 0) { (entrant, count) =>
				complete(lb.flatMap(_.around(entrant, count)))
			}
		} ~ path("get") {
			parameterMultiMap { map =>
				complete(lb.flatMap(_.get(map.getOrElse("entrant", Seq()): _*)))
			}
		} ~ path("rank") {
			parameter('score) { score =>
				complete(lb.flatMap(_.estimatedRank(score.toLong).map(_.toString)))
			}
		} ~ path("size") {
			complete(lb.flatMap(_.size).map(_.toString))
		} ~ path("range") {
			parameters('score, 'length ? 1) { (score, length) =>
				complete(lb.flatMap(_.around(score.toLong, length.toInt)))
			}
		} ~ path("page") {
			parameters('start ? 0, 'length ? 10) { (start, length) =>
				complete(lb.flatMap(_.page(start.toInt, length.toInt)))
			}
		}
	}
}

object ServiceActor {
	def props(cassandra: Cassandra) = Props(new ServiceActor(cassandra))
}

class ServiceActor(override val cassandra: Cassandra) extends Actor with Service {
	def actorRefFactory = context
	val executionContext = context.dispatcher
	def receive = runRoute(rnkrRoute)
}
