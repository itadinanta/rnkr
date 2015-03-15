package net.itadinanta.rnkr.globals

import akka.actor.ActorSystem
import net.itadinanta.common.GlobalConfig

trait ActorApp {
	val system: ActorSystem
}

trait ConfigActorApp {
	implicit val system = ActorSystem("rnkr")
}