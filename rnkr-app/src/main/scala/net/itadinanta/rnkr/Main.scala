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
import net.itadinanta.rnkr.main.Boot
import org.springframework.scala.context.function.FunctionalConfigApplicationContext
import org.springframework.scala.context.function.FunctionalConfiguration
import org.springframework.context.ApplicationContext
import net.itadinanta.common.GlobalConfig
import net.itadinanta.rnkr.backend.Cassandra

class ApplicationConfiguration extends FunctionalConfiguration {
	implicit val ctx = beanFactory.asInstanceOf[ApplicationContext]
	val cfg = GlobalConfig

	val actorSystem = bean("system") {
		val name = cfg.string("system.name")
		ActorSystem(name getOrElse "rnkr")
	} destroy {
		s =>
			s.shutdown()
			s.awaitTermination(30 seconds)
	}

	val cassandra = bean("cassandra") {
		val hosts = cfg.strings("hosts")
		val port = cfg.int("port")
		new Cassandra(hosts getOrElse Seq("127.0.0.1"), port getOrElse 9042)
	} destroy {
		_.shutdown()
	}

	val boot = bean("boot") {
		val host = cfg.string("host")
		val port = cfg.int("port")
		new Boot(cassandra(), actorSystem(), host getOrElse "localhost", port getOrElse 8080)
	} destroy {
		_.shutdown()
	}

}

object Main extends App {
	val ctx = FunctionalConfigApplicationContext(classOf[ApplicationConfiguration])

	ctx.getBean("boot", classOf[Boot]).start()

	sys.addShutdownHook {
		ctx.close()
	}

	//	val a = TreeArbiter.create(Tree.intStringTree())
	//	val done = Promise[Boolean]
	//
	//	val b = new ListBuffer[Future[Option[Row[Int, String]]]]
	//
	//	for (i <- 1 to 1000) {
	//		Future { b += a.put(i, i.toString()) map (Some(_)) }
	//		for (j <- 1 to 1000) {
	//			Future { b += a.get(i) }
	//		}
	//	}
	//	
	//	sequence(b.toList) map { r =>
	//		//		a.page(40, 10).map { r => println(r); done.success(true) }
	//		a.get(990).map { r => println(r); done.success(true) }
	//	}
	//
	//	done.future map {
	//		case _ => system.shutdown
	//	}
}
