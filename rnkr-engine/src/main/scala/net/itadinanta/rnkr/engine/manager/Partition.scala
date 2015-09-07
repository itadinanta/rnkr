package net.itadinanta.rnkr.engine.manager

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import akka.actor.Actor
import akka.actor.ActorContext
import akka.actor.ActorRefFactory
import akka.actor.Props
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout
import net.itadinanta.rnkr.backend.cassandra.Cassandra
import net.itadinanta.rnkr.engine.leaderboard.LeaderboardBuffer
import net.itadinanta.rnkr.engine.leaderboard.LeaderboardArbiter
import scala.concurrent.Promise
import net.itadinanta.rnkr.engine.leaderboard.Leaderboard
import net.itadinanta.rnkr.backend.Datastore

class Partition(datastore: Datastore)(implicit actorRefFactory: ActorRefFactory) {
	implicit val timeout: Timeout = new Timeout(30 seconds)
	sealed trait ManagerCommand
	case class Find(val name: String) extends ManagerCommand

	val partitionManager = actorRefFactory.actorOf(PartitionActor.props, "partition")

	implicit val executionContext = actorRefFactory.dispatcher

	def get(name: String): Future[Leaderboard] =
		(partitionManager ? Find(name)).mapTo[Leaderboard]

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
			registry.getOrElseUpdate(name, new PersistentLeaderboard(name, datastore, context)).leaderboard
	}
}