package net.itadinanta.rnkr.manager

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.DurationConversions._

import akka.actor.Actor
import akka.actor.ActorContext
import akka.actor.ActorRefFactory
import akka.actor.Props
import akka.pattern.ask
import akka.util.Timeout
import net.itadinanta.rnkr.arbiter.TreeArbiter
import net.itadinanta.rnkr.tree.Tree
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit

sealed trait ManagerCommand

class Manager[K, V](constructor: => Tree[K, V])(implicit actorRefFactory: ActorRefFactory) {
	val duration = FiniteDuration(30, TimeUnit.SECONDS)
	implicit val timeout: Timeout = new Timeout(duration)
	case class Find(val name: String) extends ManagerCommand
	val manager = actorRefFactory.actorOf(ManagerActor.props)

	def get(name: String) = (manager ? Find(name)).mapTo[TreeArbiter[K, V]]

	object ManagerActor {
		def props = Props(new ManagerActor)
	}

	class ManagerActor extends Actor {
		val registry = mutable.Map[String, TreeArbiter[K, V]]()
		def receive() = {
			case Find(name) => sender() ! find(name)
		}

		def find(name: String) = registry getOrElseUpdate (name, TreeArbiter.create(constructor))
	}
}