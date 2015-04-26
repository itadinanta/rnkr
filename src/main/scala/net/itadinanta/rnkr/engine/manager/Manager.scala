package net.itadinanta.rnkr.engine.manager

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.DurationConversions._
import scala.concurrent.duration.FiniteDuration
import akka.actor.Actor
import akka.actor.ActorContext
import akka.actor.ActorRefFactory
import akka.actor.Props
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout
import net.itadinanta.rnkr.backend.Cassandra
import net.itadinanta.rnkr.engine.leaderboard.LeaderboardBuffer
import net.itadinanta.rnkr.engine.leaderboard.LeaderboardArbiter
import scala.concurrent.Promise

sealed trait ManagerCommand

class Manager[K, V](cassandra: Cassandra, constructor: () => LeaderboardBuffer)(implicit actorRefFactory: ActorRefFactory) {
	val duration = FiniteDuration(30, TimeUnit.SECONDS)
	implicit val timeout: Timeout = new Timeout(duration)
	case class Find(val name: String) extends ManagerCommand

	val manager = actorRefFactory.actorOf(ManagerActor.props, "manager")

	def get(name: String): Future[LeaderboardArbiter] = {
		implicit val executionContext = actorRefFactory.dispatcher

		val found = (manager ? Find(name)).mapTo[LeaderboardArbiter]
		found map {
			_.isEmpty
		}
		found
	}

	object ManagerActor {
		def props = Props(new ManagerActor)
	}

	class ManagerActor extends Actor {
		implicit val executionContext = context.dispatcher
		val registry = mutable.Map[String, Lifecycle]()
		def receive() = {
			case Find(name) => find(name) pipeTo sender()
		}

		def lifecycleOf(name: String) = registry.getOrElseUpdate(name, new Lifecycle(name, cassandra, constructor, context))
		def find(name: String) = lifecycleOf(name).leaderboard
	}
}