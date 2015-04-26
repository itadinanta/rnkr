package net.itadinanta.rnkr.engine.leaderboard

import akka.actor.ActorContext
import net.itadinanta.rnkr.core.arbiter.Arbiter
import net.itadinanta.rnkr.core.arbiter.ActorArbiter
import net.itadinanta.rnkr.engine.leaderboard.UpdateMode._
import akka.actor.ActorRefFactory
import scala.concurrent.Future
import akka.actor.ActorRef
import net.itadinanta.rnkr.core.arbiter.ActorGateWrapper

trait LeaderboardArbiter extends Leaderboard with Arbiter[LeaderboardBuffer] {
	override def size = rqueue(_.size)
	override def isEmpty = rqueue(_.isEmpty)
	override def get(entrant: String*) = rqueue(_.get(entrant: _*))
	override def get(score: Long, timestamp: Long) = rqueue(_.get(score, timestamp))
	override def at(rank: Long) = rqueue(_.at(rank))
	override def estimatedRank(score: Long) = rqueue(_.estimatedRank(score))
	override def around(entrant: String, length: Int) = rqueue(_.around(entrant, length))
	override def around(score: Long, length: Int) = rqueue(_.around(score, length))
	override def page(start: Long, length: Int) = rqueue(_.page(start, length))

	override def post(post: Post, updateMode: UpdateMode = BestWins) = wqueue(_.post(post, updateMode))
	override def remove(entrant: String) = wqueue(_.remove(entrant))
	override def clear() = wqueue(_.clear())
}

object LeaderboardArbiter {
	def create(t: LeaderboardBuffer, context: ActorRefFactory): Leaderboard = new ActorArbiter(t, context) with LeaderboardArbiter
	def wrap(gate: ActorRef): Leaderboard = new ActorGateWrapper[LeaderboardBuffer](gate) with LeaderboardArbiter
}

