package net.itadinanta.rnkr.engine.leaderboard

import akka.actor.ActorRef
import akka.pattern.ask
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import akka.util.Timeout

class LeaderboardActorWrapper(actor: ActorRef)(implicit val executionContext: ExecutionContext)
		extends Leaderboard {
	import UpdateMode._
	import LeaderboardActor._
	implicit val timeout = Timeout(1 minute)

	override def size = (actor ? Size).mapTo[Int]
	override def isEmpty = (actor ? IsEmpty).mapTo[Boolean]
	override def get(entrant: String*) = (actor ? Lookup(entrant: _*)).mapTo[Seq[Entry]]
	override def get(score: Long, timestamp: Long) = (actor ? Get(score, timestamp)).mapTo[Option[Entry]]
	override def at(rank: Long) = (actor ? At(rank)).mapTo[Option[Entry]]
	override def estimatedRank(score: Long) = (actor ? EstimatedRank(score)).mapTo[Long]
	override def around(entrant: String, length: Int) = (actor ? Nearby(entrant, length)).mapTo[Seq[Entry]]
	override def around(score: Long, length: Int) = (actor ? Around(score, length)).mapTo[Seq[Entry]]
	override def page(start: Long, length: Int) = (actor ? Page(start, length)).mapTo[Seq[Entry]]

	override def export() = (actor ? Export).mapTo[Snapshot]

	override def post(post: Post, updateMode: UpdateMode = BestWins) = (actor ? PostScore(post, updateMode)).mapTo[Update]
	override def remove(entrant: String) = (actor ? Remove(entrant)).mapTo[Update]
	override def clear() = (actor ? Clear).mapTo[Update]
}