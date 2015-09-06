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
import net.itadinanta.rnkr.engine.leaderboard.Leaderboard
import net.itadinanta.rnkr.engine.leaderboard.LeaderboardActor
import akka.actor.ActorRef
import net.itadinanta.rnkr.engine.leaderboard.LeaderboardActor.LeaderboardActorWrapper
import net.itadinanta.rnkr.engine.manager.Partition

private object Cluster {
	case class LookupShard(partitionId: String, leaderboardId: String)
	class Shard(val partitions: Map[String, Partition]) extends Actor with Logging {
		implicit val executionContext = context.dispatcher
		override def receive = {
			case LookupShard(pid, lid) => {
				info(s"Looking up ${lid}")
				val partition = partitions get pid getOrElse { partitions.get("default").get }
				partition get lid map { leaderboard =>
					context.actorOf(LeaderboardActor.props(leaderboard))
				} pipeTo sender()
			}
		}
	}

	def props(partitions: Map[String, Partition]) = Props(new Shard(partitions))
	val shardName = "leaderboard"
}

class Cluster(val actorSystem: ActorSystem, val partitions: Map[String, Partition]) {
	import Cluster._
	private implicit val timeout = Timeout(1 minute)

	private val extractEntityId: ShardRegion.IdExtractor = {
		case e @ LookupShard(partitionId, leaderboardId) => (leaderboardId, e)
	}

	private val extractShardId: ShardRegion.ShardResolver = {
		case LookupShard(_, leaderboardId) => leaderboardId
	}

	private val clusterSharding = ClusterSharding(actorSystem).start(
		typeName = Cluster.shardName,
		entryProps = Some(Cluster.props(partitions)),
		idExtractor = extractEntityId,
		shardResolver = extractShardId)

	def find(partitionId: String, leaderboardId: String): Future[Leaderboard] = {
		implicit val executionContext = actorSystem.dispatcher
		for {
			a <- (clusterSharding ? LookupShard(partitionId, leaderboardId)).mapTo[ActorRef]
		} yield LeaderboardActor.wrap(a)
	}
}