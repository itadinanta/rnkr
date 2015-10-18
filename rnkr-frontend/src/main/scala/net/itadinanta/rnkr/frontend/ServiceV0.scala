package net.itadinanta.rnkr.frontend

import akka.actor.Actor
import spray.routing._
import spray.http._
import MediaTypes._
import spray.json.DefaultJsonProtocol
import scala.concurrent.ExecutionContext
import akka.actor.ActorContext
import spray.httpx.SprayJsonSupport
import spray.httpx.marshalling.MetaMarshallers
import scala.concurrent.Future
import net.itadinanta.rnkr.core.tree.Row
import net.itadinanta.rnkr.core.tree.RankedTreeMap
import net.itadinanta.rnkr.engine.LeaderboardBuffer
import net.itadinanta.rnkr.engine.Leaderboard.UpdateMode._
import net.itadinanta.rnkr.engine.Leaderboard._
import spray.json.JsonFormat
import spray.json.JsString
import spray.json.JsValue
import scalaz.ImmutableArray
import spray.json.DeserializationException
import akka.actor.Props
import net.itadinanta.rnkr.engine.Partition
import net.itadinanta.rnkr.cluster.Cluster
import net.itadinanta.rnkr.engine.Leaderboard
import spray.routing.directives.AuthMagnet
import spray.routing.authentication.BasicAuth
import spray.routing.authentication.UserPass
import scala.language.postfixOps

trait ServiceV0 extends HttpService
		with ServiceVersion
		with SprayJsonSupport
		with DefaultJsonProtocol
		with Authenticator {
	val cluster: Cluster

	override final def version = "v0"
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

	val rnkrRoute = pathPrefix(version / Segment) { partitionName =>
		import Leaderboard._

		def routeForLeaderboard(lb: Future[Leaderboard]) = pathEnd {
			(post | put) {
				formFields('score, 'entrant, 'attachments ?, 'force ? false) { (score, entrant, attachments, force) =>
					complete(lb flatMap { _ -> PostScore(Post(score.toLong, entrant, Attachments(attachments)), if (force) LastWins else BestWins) } map { _.newEntry })
				}
			} ~ delete {
				parameter('entrant ?) {
					_ match {
						case Some(entrant) => complete(lb flatMap { _ -> Remove(entrant) map { _.oldEntry } })
						case None => complete(lb flatMap { _ -> Clear() map { _.oldEntry } })
					}
				}
			}
		} ~ path("nearby") {
			parameter('entrant, 'count ? 0) { (entrant, count) =>
				complete(lb flatMap { _ -> Nearby(entrant, count) })
			}
		} ~ path("lookup") {
			parameterMultiMap { map =>
				complete(lb flatMap { _ -> Lookup(map getOrElse ("entrant", Seq()): _*) })
			}
		} ~ path("rank") {
			parameter('score) { score =>
				complete(lb flatMap { _ -> EstimatedRank(score.toLong) map { _.toString } })
			}
		} ~ path("size") {
			complete(lb flatMap { _ -> Size() } map { _.toString })
		} ~ path("around") {
			parameters('score, 'count ? 1) { (score, count) =>
				complete(lb flatMap { _ -> Around(score.toLong, count.toInt) })
			}
		} ~ path("page") {
			parameters('start ? 0, 'length ? 10) { (start, length) =>
				complete(lb flatMap { _ -> Page(start.toInt, length.toInt) })
			}
		}

		authenticate(authenticator(partitionName)) { role =>
			pathPrefix("""[a-zA-Z0-9]+""".r) { treeId =>
				routeForLeaderboard(cluster.find(partitionName, treeId))
			}
		}
	}
}
