package net.itadinanta.rnkr.engine.leaderboard

import akka.actor.ActorContext
import net.itadinanta.rnkr.core.arbiter.Arbiter
import net.itadinanta.rnkr.core.arbiter.ActorArbiter
import net.itadinanta.rnkr.engine.leaderboard.Leaderboard.UpdateMode._
import akka.actor.ActorRefFactory
import scala.concurrent.Future
import akka.actor.ActorRef
import net.itadinanta.rnkr.core.arbiter.ActorGateWrapper
import scala.reflect.ClassTag

trait LeaderboardArbiter extends Leaderboard with Arbiter[LeaderboardBuffer] {
	import Leaderboard._
	override def ->[T](cmd: Command[T]) = cmd match {
		case c: Read[_] => rqueue(c.apply)(c.tag)
		case c: Write => wqueue(c.apply)(c.tag)
	}
}

object LeaderboardArbiter {
	def create(t: LeaderboardBuffer, context: ActorRefFactory): Leaderboard = new ActorArbiter(t, context) with LeaderboardArbiter
	def wrap(gate: ActorRef): LeaderboardArbiter = new ActorGateWrapper[LeaderboardBuffer](gate) with LeaderboardArbiter
}

