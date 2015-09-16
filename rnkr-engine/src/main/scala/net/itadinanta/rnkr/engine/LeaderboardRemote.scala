package net.itadinanta.rnkr.engine

import grizzled.slf4j.Logging
import scala.concurrent.ExecutionContext
import akka.util.Timeout
import scala.concurrent.duration._
import akka.actor._
import akka.pattern._

import Leaderboard.Command

object LeaderboardRemote {
	private class LeaderboardActorRef(val actor: ActorRef)(implicit val executionContext: ExecutionContext)
			extends Leaderboard {
		import Leaderboard._
		implicit val timeout = Timeout(1 minute)
		override def ->[T](cmd: Command[T]) = (actor ? cmd) mapTo cmd.tag
	}

	private class LeaderboardActor(val target: Leaderboard) extends Actor with Logging {
		implicit val executionContext = context.dispatcher
		override def receive = {
			case c: Leaderboard.Command[_] => target -> c pipeTo sender()
			case other => error(s"Unknown message ${other}")
		}
	}

	def props(target: Leaderboard) = Props(new LeaderboardActor(target))

	def actorFor(target: Leaderboard)(implicit context: ActorRefFactory) =
		context.actorOf(props(target))

	def apply(actorRef: ActorRef)(implicit executionContext: ExecutionContext): Leaderboard =
		new LeaderboardActorRef(actorRef)
}

