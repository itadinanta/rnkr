package net.itadinanta.rnkr.frontend

import akka.actor.Actor
import spray.routing._
import spray.http._
import MediaTypes._
import spray.json.DefaultJsonProtocol
import scala.concurrent.ExecutionContext
import net.itadinanta.rnkr.manager.Manager
import net.itadinanta.rnkr.tree.Tree
import akka.actor.ActorContext
import spray.httpx.SprayJsonSupport
import spray.httpx.marshalling.MetaMarshallers
import scala.concurrent.Future
import net.itadinanta.rnkr.tree.Row
import net.itadinanta.rnkr.backend.CassandraCluster

trait Service extends HttpService with SprayJsonSupport with DefaultJsonProtocol {
	implicit val executionContext: ExecutionContext
	implicit val jsonRows = jsonFormat3(Row[Long, String])

	val manager = new Manager[Long, String](Tree.longStringTree())

	val rnkrRoute = pathPrefix("rnkr" / "tree" / """[a-zA-Z0-9]+""".r) { treeId =>
		val tree = manager.get(treeId)
		path("set" | "post") {
			(post | put) {
				formFields('score, 'entrant) { (score, entrant) =>
					complete(tree.flatMap(_.put(score.toLong, entrant)))
				}
			}
		} ~ path("get") {
			parameter('score) { score =>
				complete(tree.flatMap(_.get(score.toLong)))
			}
		} ~ path("rank") {
			parameter('score) { score =>
				complete(tree.flatMap(_.rank(score.toLong).map(_.toString)))
			}
		} ~ path("size") {
			complete(tree.flatMap(_.size).map(_.toString))
		} ~ path("range") {
			parameters('score, 'length ? 1) { (score, length) =>
				complete(tree.flatMap(_.range(score.toLong, length.toInt)))
			}
		} ~ path("page") {
			parameters('start ? 0, 'length ? 10) { (start, length) =>
				complete(tree.flatMap(_.page(start.toInt, length.toInt)))
			}
		}
	}
}

class ServiceActor extends Actor with Service {
	def actorRefFactory = context
	val executionContext = context.dispatcher
	def receive = runRoute(rnkrRoute)
}
