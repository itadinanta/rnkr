package net.itadinanta.rnkr

import net.itadinanta.rnkr.arbiter.Arbiter
import net.itadinanta.rnkr.tree.SeqTree
import net.itadinanta.rnkr.tree.Tree
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.dispatch.Futures
import scala.concurrent.Future
import scala.concurrent.Future._
import scala.concurrent.Promise
import scala.collection.mutable.ListBuffer
import net.itadinanta.rnkr.tree.Row
import net.itadinanta.rnkr.arbiter.TreeArbiter
import org.springframework.context.support.ClassPathXmlApplicationContext
import org.springframework.beans.factory.InitializingBean
import net.itadinanta.rnkr.main.Boot
import org.springframework.scala.context.function.FunctionalConfigApplicationContext
import org.springframework.scala.context.function.FunctionalConfiguration
import org.springframework.context.ApplicationContext
import net.itadinanta.common.GlobalConfig

class ApplicationConfiguration extends FunctionalConfiguration {
	implicit val ctx = beanFactory.asInstanceOf[ApplicationContext]
	val cfg = GlobalConfig

	val actorSystem = bean("system") {
		val name = cfg.getOptionalString("system.name")
		ActorSystem(name getOrElse "rnkr")
	}

	val boot = bean("boot") {
		val host = cfg.getOptionalString("port")
		val port = cfg.getOptionalInt("port")
		new Boot(actorSystem(), host getOrElse "localhost", port getOrElse 8080)
	}

}

object Main extends App {
	implicit val system = ActorSystem("node")
	implicit val executionContext = system.dispatchers.lookup("main-app-dispatcher")

	val ctx = FunctionalConfigApplicationContext(classOf[ApplicationConfiguration])
	ctx.getBean("boot", classOf[Boot]).start()

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
