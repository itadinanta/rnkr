package net.itadinanta.rnkr.backend.cassandra

import com.datastax.driver.core.{ ProtocolOptions, Cluster }
import net.itadinanta.rnkr.engine.LeaderboardBuffer
import akka.actor.Props
import net.itadinanta.rnkr.backend.Metadata
import scala.collection.JavaConversions
import com.datastax.driver.core.schemabuilder.SchemaBuilder._
import com.datastax.driver.core.DataType
import com.datastax.driver.core.schemabuilder.TableOptions.CompressionOptions
import com.datastax.driver.core.schemabuilder.Create
import grizzled.slf4j.Logging

class Cassandra(val hosts: Seq[String] = Seq("localhost"), val port: Int = 9042) {
	import scala.collection.JavaConversions._

	val cluster: Cluster = Cluster.builder().
		addContactPoints(hosts: _*).
		withCompression(ProtocolOptions.Compression.SNAPPY).
		withPort(port).
		build()

	def shutdown() { cluster.close() }
}

object Cassandra {
	class Datastore(val cassandra: Cassandra, val keyspace: String) extends net.itadinanta.rnkr.backend.Datastore with Logging {
		val autoCreate = true // TODO: configure me
		val replicationFactor = 1 // TODO: configure me!
		val replicationStrategy = "SimpleStrategy" // TODO: configure me!
		val initSession = cassandra.cluster.connect()
		if (autoCreate) {
			try {
				// synchronous execution
				// TODO: leak?
				initSession.execute(s"""| CREATE KEYSPACE IF NOT EXISTS "${keyspace.trim}"
																| WITH replication = {
																|   'class':'${replicationStrategy}',
																|   'replication_factor': ${replicationFactor}
																| }""".stripMargin('|'))

				initSession.execute(s"""USE "${keyspace}"""")

				def create(name: String, compact: Boolean = true)(addColumns: Create => Create) = {
					info(s"Creating ${name}")
					val statement = addColumns(createTable(name).ifNotExists())
					// TODO: replication
					val options = statement.withOptions().compressionOptions(lz4())
					if (compact) options.compactStorage()
					// TODO: leak?
					initSession.execute(statement)
				}

				create("wal") {
					_.addPartitionKey("id", DataType.text())
						.addPartitionKey("watermark", DataType.bigint())
						.addClusteringColumn("seq", DataType.bigint())
						.addColumn("scoredata", DataType.text())
				}
				create("watermarks") {
					_.addPartitionKey("id", DataType.text())
						.addClusteringColumn("watermark", DataType.bigint())
						.addColumn("pages", DataType.cint())
				}
				create("pages", compact = false) {
					_.addPartitionKey("id", DataType.text())
						.addPartitionKey("watermark", DataType.bigint())
						.addPartitionKey("page", DataType.cint())
						.addColumn("scoredata", DataType.text())
				}
				create("metadata", compact = false) {
					_.addPartitionKey("id", DataType.text())
						.addColumn("comment", DataType.text())
						.addColumn("encoding", DataType.text())
						.addColumn("page_size", DataType.cint())
						.addColumn("sorting", DataType.text())
						.addColumn("wal_size_limit", DataType.cint())
						.addColumn("wal_time_limit", DataType.bigint())
				}
			} catch {
				case e: Throwable => error(s"""Error in keyspace initialization of keyspace "${keyspace}"""", e)
			} finally {
				initSession.close()
			}
		}
		import CassandraStorage._
		override def readerProps(id: String) =
			Props(new CassandraReader(cassandra, keyspace, this, id))
		override def writerProps(id: String, watermark: Long, metadata: Metadata) =
			Props(new CassandraWriter(cassandra, keyspace, this, id, watermark, metadata))
	}
}