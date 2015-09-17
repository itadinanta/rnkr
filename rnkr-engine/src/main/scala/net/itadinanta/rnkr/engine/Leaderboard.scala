package net.itadinanta.rnkr.engine

import scala.concurrent.Future
import scalaz.ImmutableArray
import scala.reflect._
import net.itadinanta.rnkr.backend.Replay

object Leaderboard {
	object UpdateMode extends Enumeration {
		val BestWins, LastWins = Value
	}
	import UpdateMode._

	class Attachments(val data: ImmutableArray[Byte]) extends AnyVal

	object Attachments {
		def apply(data: Array[Byte]) = new Attachments(ImmutableArray.fromArray(data))
		def apply(s: String): Attachments = new Attachments(ImmutableArray.fromArray(s.getBytes("UTF8")))
		def apply(s: Option[String]): Option[Attachments] = s map { Attachments(_) }
	}

	case class Entry(score: Long, timestamp: Long, entrant: String, rank: Long, attachments: Option[Attachments])
	case class Post(score: Long, entrant: String, attachments: Option[Attachments])
	case class Update(timestamp: Long, hasChanged: Boolean, oldEntry: Option[Entry], newEntry: Option[Entry])
	case class Snapshot(timestamp: Long, entries: Seq[Entry])

	private val ctInt = classTag[Int]
	private val ctLong = classTag[Long]
	private val ctBoolean = classTag[Boolean]
	private val ctUpdate = classTag[Update]
	private val ctSeqEntry = classTag[Seq[Entry]]
	private val ctOptionEntry = classTag[Option[Entry]]
	private val ctSnapshot = classTag[Snapshot]

	sealed trait Command[T] { def tag: ClassTag[T] }

	sealed trait Read[T] extends Command[T]
	sealed trait ReadInt extends Read[Int] { def tag = ctInt }
	sealed trait ReadLong extends Read[Long] { def tag = ctLong }
	sealed trait ReadBoolean extends Read[Boolean] { def tag = ctBoolean }
	sealed trait ReadSeqEntry extends Read[Seq[Entry]] { def tag = ctSeqEntry }
	sealed trait ReadOptionEntry extends Read[Option[Entry]] { def tag = ctOptionEntry }
	sealed trait ReadSnapshot extends Read[Snapshot] { def tag = ctSnapshot }

	sealed trait Write extends Command[Update] { def tag = ctUpdate }

	case class Size() extends ReadInt
	case class IsEmpty() extends ReadBoolean
	case class Lookup(entrant: String*) extends ReadSeqEntry
	case class Get(score: Long, timestamp: Long) extends ReadOptionEntry
	case class At(rank: Long) extends ReadOptionEntry
	case class EstimatedRank(score: Long) extends ReadLong
	case class Nearby(entrant: String, length: Int) extends ReadSeqEntry
	case class Around(score: Long, length: Int) extends ReadSeqEntry
	case class Page(start: Long, length: Int) extends ReadSeqEntry

	case class Export() extends ReadSnapshot

	case class PostScore(post: Post, updateMode: UpdateMode.Value = BestWins) extends Write
	case class Remove(entrant: String) extends Write
	case class Clear() extends Write

	case class Envelope[T >: Command[_]](val id: String, val payload: T)

	abstract trait Decorator extends Leaderboard {
		protected[this] val target: Leaderboard
		def decorate[T]: PartialFunction[Command[T], Future[T]]
		override final def ->[T](cmd: Command[T]): Future[T] =
			decorate[T].applyOrElse(cmd, (c: Command[T]) => target -> c)
	}

}

trait Leaderboard {
	def ->[T](cmd: Leaderboard.Command[T]): Future[T]
}
