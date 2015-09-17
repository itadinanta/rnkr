package net.itadinanta.rnkr.engine

import net.itadinanta.rnkr.core.arbiter.Arbiter
import akka.pattern._
import akka.actor._
import net.itadinanta.rnkr.core.arbiter.ActorArbiter
import net.itadinanta.rnkr.core.arbiter.ActorGateWrapper
import Leaderboard._
import net.itadinanta.rnkr.core.arbiter.Gate
/**
 * Implements the leaderboard contract on top of an Arbiter and a mutable
 *   pre-populated LeaderboardBuffer
 */
sealed trait ConcurrentLeaderboard extends Leaderboard with Arbiter[LeaderboardBuffer] {
	import Leaderboard._
	override def ->[T](cmd: Command[T]) = cmd match {
		case c: Read[_] => rqueue(_ -> c)(c.tag)
		case c: Write => wqueue(_ -> c)(c.tag)
	}
}

object ConcurrentLeaderboard {
	/**
	 * Wraps a non-concurrent mutable leaderboard buffer into an Arbiter which is bound to an actor
	 *
	 * @param buffer the LeaderboardBuffer that needs to be concurrent-safe
	 * @param name the name of the actor that will be created to implement the Arbiter
	 * @param context the parent of the actor
	 */
	def apply(buffer: LeaderboardBuffer, name: String)(implicit context: ActorRefFactory): Leaderboard =
		new ActorArbiter(buffer, name) with ConcurrentLeaderboard
}

