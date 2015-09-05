package net.itadinanta.rnkr.engine.leaderboard

import akka.pattern.ask
import akka.pattern.pipe
import akka.actor.Actor
import grizzled.slf4j.Logging
import scala.concurrent.Future
import scala.reflect.ClassTag
import akka.actor.ActorRef
import scala.concurrent.ExecutionContext
import akka.util.Timeout
import scala.concurrent.duration._
import akka.actor.Props

object LeaderboardActor {
	sealed trait Cmd[T] {
		def f(l: Leaderboard): Future[T]
		def now(l: LeaderboardBuffer): T
		def apply(l: Leaderboard): Future[T] = f(l)
		def apply(l: LeaderboardBuffer): T = now(l)
	}

	import UpdateMode._
	case class Size() extends Cmd[Int] {
		override def f(l: Leaderboard) = l.size
		override def now(l: LeaderboardBuffer) = l.size
	}
	case class IsEmpty() extends Cmd[Boolean] {
		override def f(l: Leaderboard) = l.isEmpty
		override def now(l: LeaderboardBuffer) = l.isEmpty
	}
	case class Lookup(entrant: String*) extends Cmd[Seq[Entry]] {
		override def f(l: Leaderboard) = l.get(entrant: _*)
		override def now(l: LeaderboardBuffer) = l.get(entrant: _*)
	}
	case class Get(score: Long, timestamp: Long) extends Cmd[Option[Entry]] {
		override def f(l: Leaderboard) = l.get(score, timestamp)
		override def now(l: LeaderboardBuffer) = l.get(score, timestamp)
	}
	case class At(rank: Long) extends Cmd[Option[Entry]] {
		override def f(l: Leaderboard) = l.at(rank)
		override def now(l: LeaderboardBuffer) = l.at(rank)
	}
	case class EstimatedRank(score: Long) extends Cmd[Long] {
		override def f(l: Leaderboard) = l.estimatedRank(score)
		override def now(l: LeaderboardBuffer) = l.estimatedRank(score)
	}
	case class Nearby(entrant: String, length: Int) extends Cmd[Seq[Entry]] {
		override def f(l: Leaderboard) = l.around(entrant, length)
		override def now(l: LeaderboardBuffer) = l.around(entrant, length)
	}
	case class Around(score: Long, length: Int) extends Cmd[Seq[Entry]] {
		override def f(l: Leaderboard) = l.around(score, length)
		override def now(l: LeaderboardBuffer) = l.around(score, length)
	}
	case class Page(start: Long, length: Int) extends Cmd[Seq[Entry]] {
		override def f(l: Leaderboard) = l.page(start, length)
		override def now(l: LeaderboardBuffer) = l.page(start, length)
	}

	case class Export() extends Cmd[Snapshot] {
		override def f(l: Leaderboard) = l.export()
		override def now(l: LeaderboardBuffer) = l.export()
	}

	case class PostScore(post: Post, updateMode: UpdateMode = BestWins) extends Cmd[Update] {
		override def f(l: Leaderboard) = l.post(post, updateMode)
		override def now(l: LeaderboardBuffer) = l.post(post, updateMode)
	}
	case class Remove(entrant: String) extends Cmd[Update] {
		override def f(l: Leaderboard) = l.remove(entrant)
		override def now(l: LeaderboardBuffer) = l.remove(entrant)
	}
	case class Clear() extends Cmd[Update] {
		override def f(l: Leaderboard) = l.clear()
		override def now(l: LeaderboardBuffer) = l.clear()
	}

	case class CmdEnvelope[T >: Cmd[_]](val id: String, val payload: T)

	class LeaderboardActorWrapper(actor: ActorRef)(implicit val executionContext: ExecutionContext)
			extends Leaderboard {
		import UpdateMode._
		implicit val timeout = Timeout(1 minute)

		override def size = (actor ? Size()).mapTo[Int]
		override def isEmpty = (actor ? IsEmpty()).mapTo[Boolean]
		override def get(entrant: String*) = (actor ? Lookup(entrant: _*)).mapTo[Seq[Entry]]
		override def get(score: Long, timestamp: Long) = (actor ? Get(score, timestamp)).mapTo[Option[Entry]]
		override def at(rank: Long) = (actor ? At(rank)).mapTo[Option[Entry]]
		override def estimatedRank(score: Long) = (actor ? EstimatedRank(score)).mapTo[Long]
		override def around(entrant: String, length: Int) = (actor ? Nearby(entrant, length)).mapTo[Seq[Entry]]
		override def around(score: Long, length: Int) = (actor ? Around(score, length)).mapTo[Seq[Entry]]
		override def page(start: Long, length: Int) = (actor ? Page(start, length)).mapTo[Seq[Entry]]

		override def export() = (actor ? Export()).mapTo[Snapshot]

		override def post(post: Post, updateMode: UpdateMode = BestWins) = (actor ? PostScore(post, updateMode)).mapTo[Update]
		override def remove(entrant: String) = (actor ? Remove(entrant)).mapTo[Update]
		override def clear() = (actor ? Clear()).mapTo[Update]
	}

	def props(target: Leaderboard) = Props(new LeaderboardActor(target))

	def wrap(actorRef: ActorRef)(implicit executionContext: ExecutionContext) = new LeaderboardActorWrapper(actorRef)
}

class LeaderboardActor(val target: Leaderboard) extends Actor with Logging {
	implicit val executionContext = context.dispatcher
	override def receive = {
		case c: LeaderboardActor.Cmd[_] => c(target) pipeTo sender()
		case other =>
			println(s"Unknown message ${other}")
	}
}