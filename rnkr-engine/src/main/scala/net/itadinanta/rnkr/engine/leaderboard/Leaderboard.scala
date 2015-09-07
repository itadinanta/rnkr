package net.itadinanta.rnkr.engine.leaderboard

import scala.concurrent.Future
import scalaz.ImmutableArray
import scala.reflect._

object Leaderboard {
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

	sealed trait Command[T] {
		implicit val tag: ClassTag[T]
		def apply(l: LeaderboardBuffer): T
	}
	sealed trait Read[T] extends Command[T]
	sealed trait Write extends Command[Update] {
		override val tag = classTag[Update]
	}

	case class Size() extends Read[Int] {
		override val tag = classTag[Int]
		def apply(l: LeaderboardBuffer) = l.size
	}
	case class IsEmpty() extends Read[Boolean] {
		override val tag = classTag[Boolean]
		def apply(l: LeaderboardBuffer) = l.isEmpty
	}
	case class Lookup(entrant: String*) extends Read[Seq[Entry]] {
		override val tag = classTag[Seq[Entry]]
		def apply(l: LeaderboardBuffer) = l.lookup(entrant: _*)
	}
	case class Get(score: Long, timestamp: Long) extends Read[Option[Entry]] {
		override val tag = classTag[Option[Entry]]
		def apply(l: LeaderboardBuffer) = l.get(score, timestamp)
	}
	case class At(rank: Long) extends Read[Option[Entry]] {
		override val tag = classTag[Option[Entry]]
		def apply(l: LeaderboardBuffer) = l.at(rank)
	}
	case class EstimatedRank(score: Long) extends Read[Long] {
		override val tag = classTag[Long]
		def apply(l: LeaderboardBuffer) = l.estimatedRank(score)
	}
	case class Nearby(entrant: String, length: Int) extends Read[Seq[Entry]] {
		override val tag = classTag[Seq[Entry]]
		def apply(l: LeaderboardBuffer) = l.nearby(entrant, length)
	}
	case class Around(score: Long, length: Int) extends Read[Seq[Entry]] {
		override val tag = classTag[Seq[Entry]]
		def apply(l: LeaderboardBuffer) = l.around(score, length)
	}
	case class Page(start: Long, length: Int) extends Read[Seq[Entry]] {
		override val tag = classTag[Seq[Entry]]
		def apply(l: LeaderboardBuffer) = l.page(start, length)
	}

	case class Export() extends Read[Snapshot] {
		override val tag = classTag[Snapshot]
		def apply(l: LeaderboardBuffer) = l.export()
	}

	import UpdateMode._
	case class PostScore(post: Post, updateMode: UpdateMode = BestWins) extends Write {
		def apply(l: LeaderboardBuffer) = l.post(post, updateMode)
	}
	case class Remove(entrant: String) extends Write {
		def apply(l: LeaderboardBuffer) = l.remove(entrant)
	}
	case class Clear() extends Write {
		def apply(l: LeaderboardBuffer) = l.clear()
	}

	case class Envelope[T >: Command[_]](val id: String, val payload: T)
}

trait Leaderboard {
	def ->[T](cmd: Leaderboard.Command[T]): Future[T]
}

abstract class LeaderboardDecorator(protected[this] val target: Leaderboard) extends Leaderboard {
	override def ->[T](cmd: Leaderboard.Command[T]): Future[T] = target -> cmd
}
