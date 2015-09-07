package net.itadinanta.rnkr.engine

import grizzled.slf4j.Logging
import scala.concurrent.ExecutionContext
import akka.util.Timeout
import scala.concurrent.duration._
import akka.actor._
import akka.pattern._

import Leaderboard.Command

object LeaderboardActor {
	private class LeaderboardActorWrapper(actor: ActorRef)(implicit val executionContext: ExecutionContext)
			extends Leaderboard {
		import Leaderboard._
		implicit val timeout = Timeout(1 minute)
		override def ->[T](cmd: Command[T]) = (actor ? cmd) mapTo cmd.tag
	}

	def props(target: Leaderboard) = Props(new LeaderboardActor(target))

	def wrap(actorRef: ActorRef)(implicit executionContext: ExecutionContext): Leaderboard =
		new LeaderboardActorWrapper(actorRef)
}

class LeaderboardActor(val target: Leaderboard) extends Actor with Logging {
	implicit val executionContext = context.dispatcher
	import Leaderboard._
	override def receive = {
		case c: Command[_] =>
			implicit val tag = c.tag
			target -> c pipeTo sender()
		case other => error(s"Unknown message ${other}")
	}
}