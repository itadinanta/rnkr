package net.itadinanta.rnkr.engine

import scala.concurrent.Future
import scalaz.ImmutableArray
import scala.reflect._
import net.itadinanta.rnkr.backend.Replay

object Leaderboard {
	/** What to do with the new score for an existing entity if there is already one */
	object UpdateMode extends Enumeration {
		/** only update if the new score is better than the existing one */
		val BestWins = Value
		/** always replace the old score with the new one */
		val LastWins = Value
	}
	import UpdateMode._

	/**
	 * An arbitrary optional binary attachment that is associated to
	 * each score post and entry
	 *
	 * @param data an optional opaque blob
	 */
	class Attachments(val data: ImmutableArray[Byte]) extends AnyVal

	object Attachments {
		/** Creates an [[Attachments]] instance from a byte array */
		def apply(data: Array[Byte]) = new Attachments(ImmutableArray.fromArray(data))
		/** Creates an [[Attachments]] instance from a string with UTF8 encoding */
		def apply(s: String): Attachments = new Attachments(ImmutableArray.fromArray(s.getBytes("UTF8")))
		/** Creates an optional [[Attachments]] instance from an optional string */
		def apply(s: Option[String]): Option[Attachments] = s map { Attachments(_) }
	}

	/**
	 * A row in a leaderboard
	 *
	 *  @param score the score associated to this row. Leaderboards are always sorted by score. By
	 *    default the order is ascending.
	 *
	 *  @param timestamp a unix-epoch-like timestamp of the instant when the score was added.
	 *    If two scores are identical then priority is given to the smallest (earlier)
	 *    timestamp. The timestamps are guaranteed to be unique and monotonic.
	 *
	 *  @param entrant the identifier (or key) of the entrant to which the score is attached
	 *    e.g. a player id or name. Entrants are unique per leaderboard. Multiple [Post posts]
	 *    from the same entrant are condensed into a single Entry
	 *
	 *  @param rank the absolute rank (or position, or index) of the row in the leaderboard, 1-based
	 *
	 *  @param attachments optional user data
	 */
	case class Entry(score: Long, timestamp: Long, entrant: String, rank: Long, attachments: Option[Attachments])
	/**
	 * Encapsulates a score update request for a given entrant in a leaderboard
	 *
	 * @param score
	 *
	 * @param entrant the identifier, or key, of the entrant to which the score is attached
	 *   e.g. a player id or name. Entrants are unique per leaderboard. Multiple [Post posts]
	 *   from the same entrant are condensed into a single Entry
	 *
	 * @param attachments optional user data
	 */
	case class Post(score: Long, entrant: String, attachments: Option[Attachments])
	/**
	 * The result of a score update request after the fact
	 *
	 * @param timestamp the unique timestamp assigned to the request, used to disambiguate identical
	 *   scores. Smallest timestamps are always considered better.
	 *
	 * @param hasChanged is true if the score has been updated, false otherwise
	 *
	 * @param oldEntry the value previously stored, None if there wasn't one (first post for the entrant
	 *   in the leaderboard)
	 *
	 * @param newEntry the value with which the oldEntry was replaced. None if the old
	 *   entry was not replaced.
	 */
	case class Update(timestamp: Long, hasChanged: Boolean, oldEntry: Option[Entry], newEntry: Option[Entry])
	/**
	 * The full state of a leaderboard. Used to persist.
	 *
	 * @param timestamp the time at which the snapshot was taken
	 *
	 * @param a sorted Seq containing all entries all the entries in the leaderboard from top
	 *   to bottom
	 */
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

	/** Command to read the size of the leaderboard */
	case class Size() extends ReadInt
	/** Command to verify if the leaderboard contains any entries */
	case class IsEmpty() extends ReadBoolean
	/**
	 * Command to retrieve a Seq of Entries matching a collection of entrants
	 *
	 *  @param entrant a vararg list of entrants. If the entrant has no score in the leaderboard,
	 *    the result value is skipped
	 */
	case class Lookup(entrant: String*) extends ReadSeqEntry
	/**
	 * Command to retrieve a specific Entry matching score and timestamp. Returns None if there is no
	 *  exact match.
	 *
	 *  @param score the score to look for
	 *  @param timestamp the associated timestamp
	 */
	case class Get(score: Long, timestamp: Long) extends ReadOptionEntry
	/**
	 * Command to retrieve an entry at the given rank
	 *
	 *  @param
	 */
	case class At(rank: Long) extends ReadOptionEntry
	/**
	 * Command to estimate at which rank a given score would be inserted,
	 *   comparing the given score with the existing ones. Estimate is correct only if there
	 *   are no ongoing or queued operations in flight.
	 *
	 * @param score the score to guess the rank for
	 */
	case class EstimatedRank(score: Long) extends ReadLong
	/**
	 * Command to get the vicinity of an entrant in the leaderboard.
	 *
	 * @param entrant the entrant to look for. If the entrant is not in the leaderboards, the result
	 *   would be empty
	 * @param length how many (max) entries above and below the given one
	 */
	case class Nearby(entrant: String, length: Int) extends ReadSeqEntry
	/**
	 * Command to get the vicinity of a score in the leaderboard.
	 *
	 * @param score the score to look for. If the score is not in the leaderboard, the closest
	 *   score below it will take its place
	 * @param length how many (max) entries above and below the found one
	 */
	case class Around(score: Long, length: Int) extends ReadSeqEntry
	/**
	 * Command to get a segment of the leaderboard starting from a rank and going down.
	 *
	 * @param start the first rank to look for
	 * @param length the total length of the page, including the start
	 */
	case class Page(start: Long, length: Int) extends ReadSeqEntry
	/**
	 * Command to take a snapshot of the leaderboard. While taking snapshots all update requests are
	 * queued up. This may be a lengthy operation so it is used sparingly by the persistence
	 * mechanism.
	 */
	case class Export() extends ReadSnapshot
	/**
	 * Command to attempt to post a new score
	 *
	 * @param post the new score's parameter. A timestamp is implicitly assigned internally so all
	 *   timestamp are in sequential order
	 * @param updateMode is a directive to specify whether the an existing score for the entrant of
	 *   the post should be updated only when better, or unconditionally
	 *
	 */
	case class PostScore(post: Post, updateMode: UpdateMode.Value = BestWins) extends Write
	/**
	 * Command to attempt to remove an existing score
	 *
	 * @param entrant the entrant to remove the score for. If the entrant does not appear in the
	 *   leaderboard, the command is ignored
	 */
	case class Remove(entrant: String) extends Write
	/**
	 * Command to remove all existing scores from a leaderboard. Use with care.
	 */
	case class Clear() extends Write

	case class Envelope[T >: Command[_]](val id: String, val payload: T)

	/** Allows overriding the behaviour of a subset of commands */
	abstract trait Decorator extends Leaderboard {
		protected[this] val target: Leaderboard

		/** Any message which is part of this function's domain gets intercepted */
		def decorate[T]: PartialFunction[Command[T], Future[T]]
		override final def ->[T](cmd: Command[T]): Future[T] =
			decorate[T].applyOrElse(cmd, (c: Command[T]) => target -> c)
	}
}

/**
 * Receives leaderboard query and manipulation messages, queues them appropriately and
 *  assign the result of the computation to the returned Future
 */
trait Leaderboard {
	/**
	 * Command processing endpoint. Commands are executed asynchronously and in parallel
	 * whenever possible. Sequential consistency is guaranteed
	 *
	 * @param cmd
	 */
	def ->[T](cmd: Leaderboard.Command[T]): Future[T]
}
