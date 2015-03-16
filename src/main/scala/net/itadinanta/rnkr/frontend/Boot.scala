package net.itadinanta.rnkr.frontend

import akka.actor.{ ActorSystem, Props }
import akka.io.IO
import spray.can.Http
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import net.itadinanta.common.Constants
import net.itadinanta.common.GlobalConfig
import net.itadinanta.rnkr.globals.ConfigActorApp
import net.itadinanta.rnkr.backend.ConfigCassandraCluster

object Boot extends App with ConfigActorApp with ConfigCassandraCluster {
	val host = GlobalConfig.getOptionalString("host")
	val port = GlobalConfig.getOptionalInt("port")

	val service = system.actorOf(Props[ServiceActor], "rnkr-service")

	implicit val timeout = Timeout(5.seconds)

	IO(Http) ? Http.Bind(service, interface = host getOrElse "localhost", port = port getOrElse 8080)
}
