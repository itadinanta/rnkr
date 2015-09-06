package net.itadinanta.rnkr.engine.leaderboard

import scala.concurrent.Future
import scalaz.ImmutableArray
import scala.reflect.ClassTag

object UpdateMode extends Enumeration {
	type UpdateMode = Value
	val BestWins, LastWins = Value
}

class Attachments(val data: ImmutableArray[Byte]) extends AnyVal;

object Attachments {
	def apply(data: Array[Byte]) = new Attachments(ImmutableArray.fromArray(data))
	def apply(s: String): Attachments = new Attachments(ImmutableArray.fromArray(s.getBytes("UTF8")))
	def apply(s: Option[String]): Option[Attachments] = s map { Attachments(_) }
}

case class Entry(score: Long, timestamp: Long, entrant: String, rank: Long, attachments: Option[Attachments])
case class Post(score: Long, entrant: String, attachments: Option[Attachments])
case class Update(timestamp: Long, hasChanged: Boolean, oldEntry: Option[Entry], newEntry: Option[Entry])
case class Snapshot(timestamp: Long, entries: Seq[Entry])

object Leaderboard {
	import UpdateMode._
	sealed trait Cmd[T] {
		implicit val tag: ClassTag[T] = implicitly
		def apply(l: LeaderboardBuffer): T
	}
	sealed trait ReadCmd[T] extends Cmd[T]
	sealed trait WriteCmd[T] extends Cmd[T]

	case class Size() extends ReadCmd[Int] { def apply(l: LeaderboardBuffer) = l.size }
	case class IsEmpty() extends ReadCmd[Boolean] { def apply(l: LeaderboardBuffer) = l.isEmpty }
	case class Lookup(entrant: String*) extends ReadCmd[Seq[Entry]] { def apply(l: LeaderboardBuffer) = l.get(entrant: _*) }
	case class Get(score: Long, timestamp: Long) extends ReadCmd[Option[Entry]] { def apply(l: LeaderboardBuffer) = l.get(score, timestamp) }
	case class At(rank: Long) extends ReadCmd[Option[Entry]] { def apply(l: LeaderboardBuffer) = l.at(rank) }
	case class EstimatedRank(score: Long) extends ReadCmd[Long] { def apply(l: LeaderboardBuffer) = l.estimatedRank(score) }
	case class Nearby(entrant: String, length: Int) extends ReadCmd[Seq[Entry]] { def apply(l: LeaderboardBuffer) = l.around(entrant, length) }
	case class Around(score: Long, length: Int) extends ReadCmd[Seq[Entry]] { def apply(l: LeaderboardBuffer) = l.around(score, length) }
	case class Page(start: Long, length: Int) extends ReadCmd[Seq[Entry]] { def apply(l: LeaderboardBuffer) = l.page(start, length) }

	case class Export() extends ReadCmd[Snapshot] { def apply(l: LeaderboardBuffer) = l.export() }

	case class PostScore(post: Post, updateMode: UpdateMode = BestWins) extends WriteCmd[Update] { def apply(l: LeaderboardBuffer) = l.post(post, updateMode) }
	case class Remove(entrant: String) extends WriteCmd[Update] { def apply(l: LeaderboardBuffer) = l.remove(entrant) }
	case class Clear() extends WriteCmd[Update] { def apply(l: LeaderboardBuffer) = l.clear() }

	case class CmdEnvelope[T >: Cmd[_]](val id: String, val payload: T)
}

trait Leaderboard {
	import Leaderboard._
	import UpdateMode._

	def ->[T](cmd: Cmd[T])(implicit tag: ClassTag[T]): Future[T]
}

abstract class LeaderboardDecorator(protected[this] val target: Leaderboard) extends Leaderboard {
	import UpdateMode._
	import Leaderboard._

	def ->[T](cmd: Cmd[T])(implicit tag: ClassTag[T]): Future[T] = target -> cmd
}
