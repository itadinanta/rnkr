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

class ApplicationConfiguration extends FunctionalConfiguration {
	implicit val ctx = beanFactory.asInstanceOf[ApplicationContext]
	val cfg = ConfigFactory.load().getConfig(Constants.NAMESPACE)

	val actorSystem = bean("system") {
		val name = cfg.getString("system.name")
		ActorSystem(name)
	} destroy {
		s => Await.ready(s.terminate(), 1.minute)
	}

	val embeddedCassandra = bean(name = "cassandraServer", lazyInit = true) {
		val cassandraCfg = new File(cfg.getString("cassandra.config"))
		System.setProperty("cassandra.config", "file://" + cassandraCfg.getAbsolutePath())
		System.setProperty("cassandra-foreground", "true")
		System.setProperty("cassandra.storagedir", "target/cassandra")
		val cassandraDaemon = new CassandraDaemon()
		cassandraDaemon.activate()
		cassandraDaemon
	} destroy {
		_.destroy()
	}

	val cassandra = bean(name = "cassandra", lazyInit = true) {
		if (cfg.getBoolean("cassandra.embedded")) {
			embeddedCassandra()
		}
		val hosts = cfg.getStringList("cassandra.hosts").toList
		val port = cfg.getInt("cassandra.port")
		new Cassandra(hosts, port)
	} destroy {
		_.shutdown()
	}

	val defaultDatastore = bean[Datastore]("defaultDatastore") {
		cfg.getString("persistence.type") match {
			case "cassandra" =>
				val defaultKeyspace = cfg.getString("cassandra.default.keyspace")
				new Cassandra.Datastore(cassandra(), defaultKeyspace)
			case "blackhole" =>
				new BlackHole.Datastore()
		}
	}

	val defaultPartition = bean("defaultPartition") {
		new Partition(defaultDatastore())(actorSystem())
	}

	// "default" partition must exist
	val partitionMap = bean("partitionMap") {
		Map("default" -> defaultPartition())
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
