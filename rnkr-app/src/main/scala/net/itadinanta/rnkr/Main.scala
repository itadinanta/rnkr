package net.itadinanta.rnkr

import net.itadinanta.rnkr.core.arbiter.Arbiter
import net.itadinanta.rnkr.core.tree.SeqTree
import net.itadinanta.rnkr.core.tree.RankedTreeMap
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.dispatch.Futures
import scala.concurrent.Future
import scala.concurrent.Future._
import scala.concurrent.Promise
import scala.collection.mutable.ListBuffer
import net.itadinanta.rnkr.core.tree.Row
import org.springframework.context.support.ClassPathXmlApplicationContext
import org.springframework.beans.factory.InitializingBean
import net.itadinanta.rnkr.main.Frontend
import org.springframework.scala.context.function.FunctionalConfigApplicationContext
import org.springframework.scala.context.function.FunctionalConfiguration
import org.springframework.context.ApplicationContext
import net.itadinanta.rnkr.backend.cassandra.Cassandra
import scala.concurrent.Await
import net.itadinanta.rnkr.cluster.Cluster
import grizzled.slf4j.Logging
import net.itadinanta.rnkr.engine.Partition
import net.itadinanta.rnkr.engine.LeaderboardBuffer
import net.itadinanta.rnkr.backend.Datastore
import net.itadinanta.rnkr.backend.blackhole.BlackHole
import org.apache.cassandra.service.CassandraDaemon
import java.io.File
import com.google.common.collect.Sets
import net.itadinanta.common.Constants
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._
import com.typesafe.config.Config
import com.typesafe.config.ConfigObject

class ApplicationConfiguration extends FunctionalConfiguration {
	implicit val ctx = beanFactory.asInstanceOf[ApplicationContext]
	val cfg = ConfigFactory.load().getConfig(Constants.NAMESPACE)

	val actorSystem = bean("system") {
		val name = cfg.getString("system.name")
		ActorSystem(name)
	} destroy {
		s => Await.ready(s.terminate(), 1.minute)
	}

	val embeddedCassandra = bean(name = "cassandraServer") {
		val cassandraCfg = new File(cfg.getString("cassandra.config"))
		System.setProperty("cassandra.config", "file://" + cassandraCfg.getAbsolutePath())
		System.setProperty("cassandra-foreground", "true")
		System.setProperty("cassandra.storagedir", "target/cassandra")
		val cassandraDaemon = new CassandraDaemon()
		if (cfg.getBoolean("cassandra.embedded")) {
			cassandraDaemon.activate()
		}
		cassandraDaemon
	} destroy {
		_.destroy()
	}

	val partitionMap = bean("partitionMap") {
		val system = actorSystem()
		val partitionEntries = for {
			entry <- cfg.getObject("partitions").entrySet
			name = entry.getKey
			pcfg = entry.getValue.atPath("root").getConfig("root")
		} yield {
			val datastore = pcfg.getString("persistence.type") match {
				case "cassandra" =>
					val hosts = pcfg.getStringList("cassandra.hosts").toList
					val port = pcfg.getInt("cassandra.port")
					val cassandra = new Cassandra(hosts, port)
					val keyspace = pcfg.getString("cassandra.keyspace")
					val prefix = if (pcfg.hasPath("cassandra.prefix")) Some(pcfg.getString("cassandra.prefix")) else None
					new Cassandra.Datastore(cassandra, keyspace, prefix)
				case "blackhole" =>
					new BlackHole.Datastore()
			}

			val auth = for (authCfg <- pcfg.getObject("auth").entrySet) yield (authCfg.getKey, authCfg.getValue.atPath("value").getString("value"))

			val partition = new Partition(datastore, Map(auth.toSeq: _*))(system)
			(name, partition)
		}
		// "default" partition must exist
		Map(partitionEntries.toSeq: _*)
	}

	val cluster = bean("cluster") {
		new Cluster(actorSystem(), partitionMap())
	}

	val frontend = bean("frontend") {
		val host = cfg.getString("listen.host")
		val port = cfg.getInt("listen.port")
		new Frontend(actorSystem(), cluster(), host, port)
	} destroy {
		_.shutdown()
	}

}

object Main extends App with Logging {
	debug("Starting...")
	val ctx = FunctionalConfigApplicationContext(classOf[ApplicationConfiguration])

	ctx.getBean("frontend", classOf[Frontend]).start()

	sys.addShutdownHook {
		ctx.close()
	}
}
