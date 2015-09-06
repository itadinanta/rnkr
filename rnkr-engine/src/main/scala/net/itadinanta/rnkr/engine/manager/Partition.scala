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
import net.itadinanta.rnkr.engine.leaderboard.Leaderboard

class Partition(cassandra: Cassandra)(implicit actorRefFactory: ActorRefFactory) {
	val duration = FiniteDuration(30, TimeUnit.SECONDS)
	implicit val timeout: Timeout = new Timeout(duration)
	sealed trait ManagerCommand
	case class Find(val name: String) extends ManagerCommand

	val partitionManager = actorRefFactory.actorOf(PartitionActor.props, "partition")

	implicit val executionContext = actorRefFactory.dispatcher

	def get(name: String): Future[Leaderboard] = {
		(partitionManager ? Find(name)).mapTo[Leaderboard]
	}

	object PartitionActor {
		def props = Props(new PartitionActor)
	}

	class PartitionActor extends Actor {
		implicit val executionContext = context.dispatcher
		val registry = mutable.Map[String, PersistentLeaderboard]()
		def receive() = {
			case Find(name) => find(name) pipeTo sender()
		}

		def find(name: String) =
			registry.getOrElseUpdate(name, new PersistentLeaderboard(name, cassandra, context)).leaderboard
	}
}