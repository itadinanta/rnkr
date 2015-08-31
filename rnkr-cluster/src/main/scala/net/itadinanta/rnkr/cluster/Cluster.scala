package net.itadinanta.rnkr.cluster

import akka.actor.ActorSystem
import akka.actor.Actor
import akka.actor.Props
import akka.actor.ActorRefFactory
import akka.contrib.pattern.ShardRegion
import akka.contrib.pattern.ClusterSharding
import scala.concurrent.Future
import akka.pattern.ask
import scala.concurrent.duration._
import akka.util.Timeout
import grizzled.slf4j.Logging

private object Cluster {
	class Clusterable extends Actor with Logging {
		override def receive = {
			case s: String => {
				info(s"Responding to ${s}")
				sender() ! s
			}
		}
	}

	def props = Props(classOf[Clusterable])
	val shardName = "leaderboard"
	case class LookupShard(leaderboardId: String)
}

class Cluster(val actorSystem: ActorSystem) {
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
		entryProps = Some(Cluster.props),
		idExtractor = extractEntityId,
		shardResolver = extractShardId)

	def ping(leaderboardId: String): Future[String] =
		(clusterSharding ? LookupShard(leaderboardId)).mapTo[String]
}