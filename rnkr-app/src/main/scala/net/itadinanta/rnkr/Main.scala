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
import net.itadinanta.common.GlobalConfig
import net.itadinanta.rnkr.backend.cassandra.Cassandra
import scala.concurrent.Await
import net.itadinanta.rnkr.cluster.Cluster
import grizzled.slf4j.Logging
import net.itadinanta.rnkr.engine.Partition
import net.itadinanta.rnkr.engine.LeaderboardBuffer
import net.itadinanta.rnkr.backend.Datastore
import net.itadinanta.rnkr.backend.blackhole.BlackHole

class ApplicationConfiguration extends FunctionalConfiguration {
	implicit val ctx = beanFactory.asInstanceOf[ApplicationContext]
	val cfg = GlobalConfig

	val actorSystem = bean("system") {
		val name = cfg.string("system.name")
		ActorSystem(name)
	} destroy {
		//	Akka 2.4, not ready as of today
		//	s => Await.ready(s.terminate(), 1 minute)
		s =>
			s.shutdown()
			s.awaitTermination()
	}

	val cassandra = bean(name = "cassandra", lazyInit = true) {
		val hosts = cfg.strings("cassandra.hosts")
		val port = cfg.int("cassandra.port")
		new Cassandra(hosts, port)
	} destroy {
		_.shutdown()
	}

	val defaultDatastore = bean[Datastore]("defaultDatastore") {
		cfg.string("persistence.type") match {
			case "cassandra" =>
				val defaultKeyspace = cfg.string("cassandra.default.keyspace")
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
		val host = cfg.string("listen.host")
		val port = cfg.int("listen.port")
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
