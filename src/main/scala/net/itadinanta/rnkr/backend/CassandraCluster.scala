package net.itadinanta.rnkr.backend

import com.datastax.driver.core.{ ProtocolOptions, Cluster }
import akka.actor.ActorSystem
import net.itadinanta.common.GlobalConfig

trait CassandraCluster {
	def cluster: Cluster
}

trait ConfigCassandraCluster extends CassandraCluster {
	def system: ActorSystem

	import scala.collection.JavaConversions._
	private val port = GlobalConfig.getOptionalInt("cassandra.port")
	private val hosts = GlobalConfig.getOptionalString("cassandra.host").toList

	lazy val cluster: Cluster =
		Cluster.builder().
			addContactPoints(hosts: _*).
			withCompression(ProtocolOptions.Compression.SNAPPY).
			withPort(port.getOrElse(9042)).
			build()
}