package net.itadinanta.rnkr.engine

import scala.concurrent.Future
import scalaz.ImmutableArray
import scala.reflect._
import net.itadinanta.rnkr.backend.Replay

object Leaderboard {
	object UpdateMode extends Enumeration {
		type UpdateMode = Value
		val BestWins, LastWins = Value
	}
	import UpdateMode._

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

	sealed trait Command[T] { implicit val tag: ClassTag[T] = classTag[T] }
	sealed trait Read[T] extends Command[T]
	sealed trait Write extends Command[Update]

	case class Size() extends Read[Int]
	case class IsEmpty() extends Read[Boolean]
	case class Lookup(entrant: String*) extends Read[Seq[Entry]]
	case class Get(score: Long, timestamp: Long) extends Read[Option[Entry]]
	case class At(rank: Long) extends Read[Option[Entry]]
	case class EstimatedRank(score: Long) extends Read[Long]
	case class Nearby(entrant: String, length: Int) extends Read[Seq[Entry]]
	case class Around(score: Long, length: Int) extends Read[Seq[Entry]]
	case class Page(start: Long, length: Int) extends Read[Seq[Entry]]

	case class Export() extends Read[Snapshot]

	case class PostScore(post: Post, updateMode: UpdateMode = BestWins) extends Write
	case class Remove(entrant: String) extends Write
	case class Clear() extends Write

	case class Envelope[T >: Command[_]](val id: String, val payload: T)
}

trait Leaderboard {
	def ->[T](cmd: Leaderboard.Command[T]): Future[T]
}
