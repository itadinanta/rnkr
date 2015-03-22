package net.itadinanta.rnkr.engine.leaderboard

import akka.actor.ActorContext
import net.itadinanta.rnkr.core.arbiter.Arbiter
import net.itadinanta.rnkr.core.arbiter.ActorArbiter
import net.itadinanta.rnkr.engine.leaderboard.UpdateMode._

trait LeaderboardArbiter extends Arbiter[Leaderboard] {

	def size = rqueue(_.size)
	def isEmpty = rqueue(_.isEmpty)
	def get(entrant: String*) = rqueue(_.get(entrant: _*))
	def get(score: Long, timestamp: Long) = rqueue(_.get(score, timestamp))
	def at(rank: Long) = rqueue(_.at(rank))
	def estimatedRank(score: Long) = rqueue(_.estimatedRank(score))
	def around(entrant: String, length: Int) = rqueue(_.around(entrant, length))
	def around(score: Long, length: Int) = rqueue(_.around(score, length))
	def page(start: Long, length: Int) = rqueue(_.page(start, length))

	def post(post: Post, updateMode: UpdateMode = BestWins) = wqueue(_.post(post, updateMode))
	def remove(entrant: String) = wqueue(_.remove(entrant))
	def clear() = wqueue(_.clear())
}

object LeaderboardArbiter {
	def create(t: Leaderboard)(implicit context: ActorContext) = new ActorArbiter(t) with LeaderboardArbiter
}

