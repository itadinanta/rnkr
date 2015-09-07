package net.itadinanta.rnkr.engine

import net.itadinanta.rnkr.core.arbiter.Arbiter
import akka.pattern._
import akka.actor._
import net.itadinanta.rnkr.core.arbiter.ActorArbiter
import net.itadinanta.rnkr.core.arbiter.ActorGateWrapper
import Leaderboard._

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

