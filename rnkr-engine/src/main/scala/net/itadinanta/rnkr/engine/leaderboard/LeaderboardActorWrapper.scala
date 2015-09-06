package net.itadinanta.rnkr.engine.leaderboard

import akka.actor.ActorRef
import akka.pattern.ask
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import akka.util.Timeout
import scala.reflect.ClassTag

class LeaderboardActorWrapper(actor: ActorRef)(implicit val executionContext: ExecutionContext)
		extends Leaderboard {
	import UpdateMode._
	import Leaderboard._
	
	implicit val timeout = Timeout(1 minute)

	def ->[T](cmd: Command[T]) = (actor ? cmd).mapTo(cmd.tag)
}