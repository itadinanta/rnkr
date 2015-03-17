package net.itadinanta.rnkr.main

import akka.actor.Props
import akka.io.IO
import spray.can.Http
import akka.util.Timeout
import akka.pattern.ask
import scala.concurrent.duration._
import net.itadinanta.common.GlobalConfig
import net.itadinanta.rnkr.globals.ConfigActorApp
import net.itadinanta.rnkr.backend.ConfigCassandraCluster
import net.itadinanta.rnkr.frontend.ServiceActor
import org.springframework.context.support.ClassPathXmlApplicationContext

object Boot extends App with ConfigActorApp with ConfigCassandraCluster {
	
	val ctx = new ClassPathXmlApplicationContext("applicationContext.xml");
	ctx.refresh();
	
	val host = GlobalConfig.getOptionalString("host")
	val port = GlobalConfig.getOptionalInt("port")
	
	val service = system.actorOf(Props[ServiceActor], "rnkr-service")

	implicit val timeout = Timeout(5.seconds)

	IO(Http) ? Http.Bind(service, interface = host getOrElse "localhost", port = port getOrElse 8080)
}
