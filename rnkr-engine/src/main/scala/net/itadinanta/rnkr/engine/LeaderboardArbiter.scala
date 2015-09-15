package net.itadinanta.rnkr.engine

import net.itadinanta.rnkr.core.arbiter.Arbiter
import akka.pattern._
import akka.actor._
import net.itadinanta.rnkr.core.arbiter.ActorArbiter
import net.itadinanta.rnkr.core.arbiter.ActorGateWrapper
import Leaderboard._
import net.itadinanta.rnkr.core.arbiter.Gate

trait LeaderboardArbiter extends Leaderboard with Arbiter[LeaderboardBuffer] {
	import Leaderboard._
	override def ->[T](cmd: Command[T]) = cmd match {
		case c: Read[_] => rqueue(_ -> c)(c.tag)
		case c: Write => wqueue(_ -> c)(c.tag)
	}
}

object LeaderboardArbiter {
	def apply(buffer: LeaderboardBuffer, name: String)(implicit context: ActorRefFactory) =
		new ActorArbiter(buffer, name) with LeaderboardArbiter
}

