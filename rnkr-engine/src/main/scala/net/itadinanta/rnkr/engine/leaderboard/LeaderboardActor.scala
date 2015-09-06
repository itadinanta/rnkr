package net.itadinanta.rnkr.engine.leaderboard

import akka.pattern.ask
import akka.pattern.pipe
import akka.actor.Actor
import grizzled.slf4j.Logging
import scala.concurrent.Future
import scala.reflect.ClassTag
import akka.actor.ActorRef
import scala.concurrent.ExecutionContext
import akka.util.Timeout
import scala.concurrent.duration._
import akka.actor.Props

object LeaderboardActor {

	class LeaderboardActorWrapper(actor: ActorRef)(implicit val executionContext: ExecutionContext)
			extends Leaderboard {
		import UpdateMode._
		import Leaderboard._
		implicit val timeout = Timeout(1 minute)

		override def ->[T](cmd: Command[T])(implicit tag: ClassTag[T] = cmd.tag) = (actor ? cmd).mapTo[T]
	}

	def props(target: Leaderboard) = Props(new LeaderboardActor(target))

	def wrap(actorRef: ActorRef)(implicit executionContext: ExecutionContext) = new LeaderboardActorWrapper(actorRef)
}

class LeaderboardActor(val target: Leaderboard) extends Actor with Logging {
	implicit val executionContext = context.dispatcher
	import Leaderboard._
	override def receive = {
		case c: Command[_] =>
			implicit val tag = c.tag
			target -> c pipeTo sender()
		case other => println(s"Unknown message ${other}")
	}
}