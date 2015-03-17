package net.itadinanta.rnkr.main

import scala.concurrent.duration.DurationInt
import akka.actor.ActorSystem
import akka.actor.Props
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import grizzled.slf4j.Logger
import net.itadinanta.rnkr.frontend.ServiceActor
import net.itadinanta.rnkr.globals.ConfigActorApp
import spray.can.Http
import akka.actor.PoisonPill

class Boot(override val system: ActorSystem, val host: String, val port: Int) extends ConfigActorApp {
	val log = Logger[this.type]

	val service = system.actorOf(Props[ServiceActor], "rnkr-service")

	def start() = {
		log.debug("Starting service {}:{}", host, port)
		implicit val timeout = Timeout(5.seconds)
		IO(Http)(system) ? Http.Bind(service, interface = host, port = port)
	}

	def shutdown() {
		service ! PoisonPill
	}
}
