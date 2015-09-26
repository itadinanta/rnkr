package net.itadinanta.rnkr.engine

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import akka.actor.Actor
import akka.actor.ActorRefFactory
import akka.actor.Props
import akka.util.Timeout
import akka.pattern._
import net.itadinanta.rnkr.backend.Datastore
import scala.language.postfixOps

class Partition(val datastore: Datastore)(implicit actorRefFactory: ActorRefFactory) {
	implicit val executionContext = actorRefFactory.dispatcher
	implicit val timeout: Timeout = new Timeout(30 seconds)

	sealed trait ManagerCommand
	case class Find(val name: String) extends ManagerCommand

	val partitionManager = actorRefFactory.actorOf(PartitionActor.props, "partition")

	def get(name: String): Future[Leaderboard] =
		(partitionManager ? Find(name)).mapTo[Leaderboard]

	object PartitionActor {
		def props = Props(new PartitionActor)
	}

	class PartitionActor extends Actor {
		implicit val actorRefFactory = context
		implicit val executionContext = context.dispatcher
		val registry = mutable.Map[String, Future[Leaderboard]]()

		def find(name: String) =
			registry.getOrElseUpdate(name, PersistentLeaderboard(name, datastore))

		def receive = {
			case Find(name) => find(name) pipeTo sender()
		}

	}
}