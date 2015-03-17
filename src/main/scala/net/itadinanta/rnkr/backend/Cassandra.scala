package net.itadinanta.rnkr.backend

import com.datastax.driver.core.{ ProtocolOptions, Cluster }
import akka.actor.ActorSystem
import net.itadinanta.common.GlobalConfig

class Cassandra(val hosts: Seq[String], val port: Int) {
	import scala.collection.JavaConversions._

	val cluster: Cluster =
		Cluster.builder().
			addContactPoints(hosts: _*).
			//			withCompression(ProtocolOptions.Compression.SNAPPY).
			withPort(port).
			build()
	cluster.connect()
			
	def shutdown() { cluster.close() }
}