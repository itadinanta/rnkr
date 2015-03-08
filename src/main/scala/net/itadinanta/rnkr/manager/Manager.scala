package net.itadinanta.rnkr.manager

import akka.actor.Actor
import net.itadinanta.rnkr.arbiter.Arbiter
import scala.collection.mutable
import net.itadinanta.rnkr.arbiter.TreeArbiter
import akka.actor.ActorSystem
import net.itadinanta.rnkr.tree.Tree
import scala.concurrent.Future
import akka.actor.Props
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._

sealed trait ManagerCommand
case class Find(val name: String) extends ManagerCommand

class Manager[K, V](constructor: () => Tree[K, V])(implicit system: ActorSystem) {

	val manager = system.actorOf(Props[ManagerActor])
	implicit val timeout = Timeout(30 seconds)

	def get(name: String) = (manager ? Find(name)).mapTo[TreeArbiter[K, V]]

	class ManagerActor extends Actor {
		val registry = mutable.Map[String, TreeArbiter[K, V]]()
		def receive() = {
			case Find(name) => sender() ! find(name)
		}

		def find(name: String) = registry getOrElseUpdate (name, TreeArbiter.create[K, V](constructor()))
	}
}