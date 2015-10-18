package net.itadinanta.rnkr.frontend

import scala.language.postfixOps

import akka.actor.Actor
import akka.actor.Props
import net.itadinanta.rnkr.cluster.Cluster

object ServiceActor {
	def props(cluster: Cluster) = Props(new ServiceActor(cluster))
}

class ServiceActor(override val cluster: Cluster) extends Actor
		with ServiceV0
		with PartitionBasedAuthenticator {
	override val partitions = cluster.partitions
	override def actorRefFactory = context
	override val executionContext = context.dispatcher
	override def receive = runRoute(rnkrRoute)
}
