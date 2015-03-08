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

trait Service extends HttpService with SprayJsonSupport with DefaultJsonProtocol {
	implicit val executionContext: ExecutionContext
	val manager = new Manager[Long, String](Tree.longStringTree())(actorRefFactory)

	val rnkrRoute =
		pathPrefix("rnkr") {
			pathPrefix("tree") {
				pathPrefix("""[a-zA-Z]+""".r) { treeId =>
					path("size") {
						get {
							complete(manager.get(treeId).flatMap(_.size).map(_.toString))
						}
					}
				}
			}
		}
}

class ServiceActor extends Actor with Service {
	def actorRefFactory = context
	val executionContext = context.dispatcher
	def receive = runRoute(rnkrRoute)
}
