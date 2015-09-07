package net.itadinanta.rnkr.backend.cassandra

import com.datastax.driver.core.{ ProtocolOptions, Cluster }
import net.itadinanta.rnkr.engine.leaderboard.LeaderboardBuffer
import akka.actor.Props
import net.itadinanta.rnkr.backend.Metadata
import scala.collection.JavaConversions

class Cassandra(val hosts: Seq[String] = Seq("localhost"), val port: Int = 9042) {
	import scala.collection.JavaConversions._

	val cluster: Cluster =
		Cluster.builder().
			addContactPoints(hosts: _*).
			withCompression(ProtocolOptions.Compression.SNAPPY).
			withPort(port).
			build()
	cluster.connect()

	def shutdown() { cluster.close() }
}

object Cassandra {
	class Datastore(val cassandra: Cassandra, val keyspace: String) extends net.itadinanta.rnkr.backend.Datastore {
		import CassandraStorage._
		override def readerProps(id: String, leaderboard: LeaderboardBuffer) =
			Props(new CassandraReader(cassandra, keyspace, this, id, leaderboard))
		override def writerProps(id: String, watermark: Long, metadata: Metadata) =
			Props(new CassandraWriter(cassandra, keyspace, this, id, watermark, metadata))
	}
}