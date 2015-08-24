package net.itadinanta.rnkr.cluster

import akka.cluster.sharding.ClusterSharding
import akka.actor.ActorSystem
import akka.actor.Actor
import akka.actor.Props
import akka.actor.ActorRefFactory
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ClusterShardingSettings

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
	val extractShardId: ShardRegion.ExtractShardId = {
		case LookupShard(userId) => userId.toString
	}

	val extractEntityId: ShardRegion.ExtractEntityId = {
		case LookupShard(userId) => (userId, userId)
	}

	val clusterSharding = ClusterSharding(actorSystem).start(
		typeName = Cluster.shardName,
		entityProps = Cluster.props,
		settings = ClusterShardingSettings(actorSystem),
		extractEntityId = extractEntityId,
		extractShardId = extractShardId)

}