package net.itadinanta.rnkr.cluster

import akka.actor.ActorSystem
import akka.actor.Actor
import akka.actor.Props
import akka.actor.ActorRefFactory
import akka.contrib.pattern.ShardRegion
import akka.contrib.pattern.ClusterSharding

private object Cluster {
	class Clusterable extends Actor {
		override def receive = ???
	}
	def props = Props(classOf[Clusterable])
	val shardName = "rnkr"
	//	def lookup(implicit arf: ActorRefFactory) = actors.shard.lookup(arf, shardName)

	case class LookupShard(leaderboardId: String)
}

class Cluster(val actorSystem: ActorSystem) {
	import Cluster._
	val extractShardId: ShardRegion.ShardResolver = {
		case LookupShard(userId) => userId.toString
	}

	val extractEntityId: ShardRegion.IdExtractor = {
		case LookupShard(userId) => (userId, userId)
	}

	val clusterSharding = ClusterSharding(actorSystem).start(
		typeName = Cluster.shardName,
		entryProps = Some(Cluster.props),
		//		settings = ClusterShardingSettings(actorSystem),
		idExtractor = extractEntityId,
		shardResolver = extractShardId)

}