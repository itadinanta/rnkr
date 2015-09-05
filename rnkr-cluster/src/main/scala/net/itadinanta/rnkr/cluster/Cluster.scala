package net.itadinanta.rnkr.cluster

import akka.actor.ActorSystem
import akka.actor.Actor
import akka.actor.Props
import akka.actor.ActorRefFactory
import akka.contrib.pattern.ShardRegion
import akka.contrib.pattern.ClusterSharding
import scala.concurrent.Future
import akka.pattern.ask
import akka.pattern.pipe
import scala.concurrent.duration._
import akka.util.Timeout
import grizzled.slf4j.Logging
import net.itadinanta.rnkr.engine.manager.Partition
import net.itadinanta.rnkr.engine.leaderboard.Leaderboard
import net.itadinanta.rnkr.engine.leaderboard.LeaderboardActor
import akka.actor.ActorRef
import net.itadinanta.rnkr.engine.leaderboard.LeaderboardActor.LeaderboardActorWrapper

private object Cluster {
	class Shard(partition: Partition) extends Actor with Logging {
		implicit val executionContext = context.dispatcher
		override def receive = {
			case id: String => {
				println(s"Looking up ${id}")
				partition.get(id) map { leaderboard =>
					context.actorOf(LeaderboardActor.props(leaderboard))
				} pipeTo sender()
			}
		}
	}

	def props(partition: Partition) = Props(new Shard(partition))
	val shardName = "leaderboard"
	case class LookupShard(leaderboardId: String)
}

class Cluster(val actorSystem: ActorSystem, val partition: Partition) {
	import Cluster._
	private implicit val timeout = Timeout(1 minute)

	private val extractEntityId: ShardRegion.IdExtractor = {
		case LookupShard(leaderboardId) => (leaderboardId, leaderboardId)
	}

	private val extractShardId: ShardRegion.ShardResolver = {
		case LookupShard(leaderboardId) => leaderboardId
	}

	private val clusterSharding = ClusterSharding(actorSystem).start(
		typeName = Cluster.shardName,
		entryProps = Some(Cluster.props(partition)),
		idExtractor = extractEntityId,
		shardResolver = extractShardId)

	def find(leaderboardId: String): Future[Leaderboard] = {
		implicit val executionContext = actorSystem.dispatcher
		for {
			a <- (clusterSharding ? LookupShard(leaderboardId)).mapTo[ActorRef]
		} yield LeaderboardActor.wrap(a)
	}
}