package net.itadinanta.rnkr.main

import scala.concurrent.duration.DurationInt
import akka.actor.ActorSystem
import akka.actor.Props
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import grizzled.slf4j.Logger
import net.itadinanta.rnkr.frontend.ServiceActor
import spray.can.Http
import akka.actor.PoisonPill
import grizzled.slf4j.Logging
import net.itadinanta.rnkr.frontend.ServiceActor
import net.itadinanta.rnkr.backend.Cassandra
import net.itadinanta.rnkr.cluster.Cluster

class Frontend(val system: ActorSystem, val cluster: Cluster, val host: String, val port: Int) extends Logging {

	val service = system.actorOf(ServiceActor.props(cluster), "rnkr-service")

	def start() = {
		debug(s"Starting service ${host}:${port}")
		implicit val timeout = Timeout(5.seconds)
		IO(Http)(system) ? Http.Bind(service, interface = host, port = port)
	}

	def shutdown() {
		service ! PoisonPill
	}
}
