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
import net.itadinanta.rnkr.engine.leaderboard.Leaderboard
import net.itadinanta.rnkr.engine.leaderboard.Post
import net.itadinanta.rnkr.engine.leaderboard.Update
import net.itadinanta.rnkr.engine.leaderboard.Entry
import net.itadinanta.rnkr.engine.leaderboard.Attachments
import spray.json.JsonFormat
import spray.json.JsString
import spray.json.JsValue
import scalaz.ImmutableArray
import spray.json.DeserializationException

trait Service extends HttpService with SprayJsonSupport with DefaultJsonProtocol {
	implicit val executionContext: ExecutionContext
	implicit object AttachmentJsonFormat extends JsonFormat[Attachments] {
		def write(c: Attachments) =
			JsString(new String(c.data.toArray, "UTF8"))

		def read(value: JsValue) = value match {
			case JsString(s) => new Attachments(ImmutableArray.fromArray(s.getBytes("UTF8")))
			case _ => throw new DeserializationException("Invalid format for Attachments")
		}
	}
	implicit val jsonEntry = jsonFormat5(Entry)

	val manager = new Manager(Leaderboard())

	val rnkrRoute = pathPrefix("rnkr" / "tree" / """[a-zA-Z0-9]+""".r) { treeId =>
		val lb = manager.get(treeId)
		path("set" | "post") {
			(post | put) {
				formFields('score, 'entrant) { (score, entrant) =>
					complete(lb.flatMap(_.post(Post(score.toLong, entrant, None))).map(_.newEntry))
				}
			}
		} ~ path("around") {
			parameter('entrant, 'count ? 0) { (entrant, count) =>
				complete(lb.flatMap(_.around(entrant, count)))
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

class ServiceActor extends Actor with Service {
	def actorRefFactory = context
	val executionContext = context.dispatcher
	def receive = runRoute(rnkrRoute)
}
