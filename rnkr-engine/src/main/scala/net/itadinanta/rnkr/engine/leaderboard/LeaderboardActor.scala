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
	sealed class Cmd[T](
			private val future: Leaderboard => Future[T],
			private val now: LeaderboardBuffer => T)(implicit val tag: ClassTag[T]) {
		def apply(l: Leaderboard) = future(l)
		def apply(l: LeaderboardBuffer) = now(l)
	}

	import UpdateMode._
	case object Size extends Cmd(_.size, _.size)
	case object IsEmpty extends Cmd(_.isEmpty, _.isEmpty)
	case class Lookup(entrant: String*) extends Cmd(_.get(entrant: _*), _.get(entrant: _*))
	case class Get(score: Long, timestamp: Long) extends Cmd(_.get(score, timestamp), _.get(score, timestamp))
	case class At(rank: Long) extends Cmd(_.at(rank), _.at(rank))
	case class EstimatedRank(score: Long) extends Cmd(_.estimatedRank(score), _.estimatedRank(score))
	case class Nearby(entrant: String, length: Int) extends Cmd(_.around(entrant, length), _.around(entrant, length))
	case class Around(score: Long, length: Int) extends Cmd(_.around(score, length), _.around(score, length))
	case class Page(start: Long, length: Int) extends Cmd(_.around(start, length), _.around(start, length))

	case object Export extends Cmd(_.export(), _.export())

	case class PostScore(post: Post, updateMode: UpdateMode = BestWins) extends Cmd(_.post(post, updateMode), _.post(post, updateMode))
	case class Remove(entrant: String) extends Cmd(_.remove(entrant), _.remove(entrant))
	case class Clear() extends Cmd(_.clear(), _.clear())

	case class CmdEnvelope[T >: Cmd[_]](val id: String, val payload: T)

	class LeaderboardActorWrapper(actor: ActorRef)(implicit val executionContext: ExecutionContext)
			extends Leaderboard {
		import UpdateMode._
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

	def props(target: Leaderboard) = Props(new LeaderboardActor(target))

	def wrap(actorRef: ActorRef)(implicit executionContext: ExecutionContext) = new LeaderboardActorWrapper(actorRef)
}

class LeaderboardActor(val target: Leaderboard) extends Actor with Logging {
	implicit val executionContext = context.dispatcher
	override def receive = {
		case c: LeaderboardActor.Cmd[_] => c(target) pipeTo sender()
	}
}