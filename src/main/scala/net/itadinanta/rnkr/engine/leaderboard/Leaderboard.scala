package net.itadinanta.rnkr.engine.leaderboard

import scala.collection.mutable.Map
import net.itadinanta.rnkr.core.tree.Ordering
import net.itadinanta.rnkr.core.tree.Row
import net.itadinanta.rnkr.core.tree.RankedTreeMap
import scalaz.ImmutableArray
import java.util.Arrays

object UpdateMode extends Enumeration {
	type UpdateMode = Value
	val BestWins, LastWins = Value
}
import UpdateMode._
class Attachments(val data: ImmutableArray[Byte]) extends AnyVal;
object Attachments {
	def apply(s: String): Attachments = new Attachments(ImmutableArray.fromArray(s.getBytes("UTF8")))
	def apply(s: Option[String]): Option[Attachments] = for { v <- s } yield Attachments(v)
}
case class Entry(score: Long, timestamp: Long, entrant: String, rank: Long, attachments: Option[Attachments])
case class Post(score: Long, entrant: String, attachments: Option[Attachments])
case class Update(oldEntry: Option[Entry], newEntry: Option[Entry])

trait Leaderboard {
	def size: Int
	def isEmpty: Boolean
	def get(entrant: String*): Seq[Entry]
	def get(score: Long, timestamp: Long): Option[Entry]
	def at(rank: Long): Option[Entry]
	def estimatedRank(score: Long): Long
	def around(entrant: String, length: Int): Seq[Entry]
	def around(score: Long, length: Int): Seq[Entry]
	def page(start: Long, length: Int): Seq[Entry]

	def remove(entrant: String): Update
	def post(post: Post, updateMode: UpdateMode = BestWins): Update
	def clear(): Int
}

private[leaderboard] case class TimedScore(val score: Long, val timestamp: Long, val attachments: Option[Attachments] = None) {
	override def hashCode = ((31 * score) + timestamp).intValue
	override def equals(o: Any) = o match { case t: TimedScore => t.score == score && t.timestamp == timestamp }
}

private[leaderboard] object TimedScoreOrdering extends Ordering[TimedScore] {
	def lt(a: TimedScore, b: TimedScore): Boolean = a.score < b.score || (a.score == b.score && a.timestamp < b.timestamp)
}

object Leaderboard {
	val TimestampScale = 10000L
	def apply(): Leaderboard = new LeaderboardTreeImpl()
}

class LeaderboardTreeImpl extends Leaderboard {
	val ordering = TimedScoreOrdering
	val scoreIndex = RankedTreeMap[TimedScore, String](ordering)
	val entrantIndex = Map[String, TimedScore]()

	private[this] var _lastTime: Long = System.currentTimeMillis
	private[this] var _lastCount: Long = 0

	override def size = scoreIndex.size
	override def isEmpty = scoreIndex.isEmpty

	override def get(entrants: String*) =
		for {
			e <- entrants
			s <- entrantIndex.get(e)
			r <- scoreIndex.get(s)
		} yield Entry(s.score, s.timestamp, e, r.rank, s.attachments)

	override def get(score: Long, timestamp: Long) =
		for {
			r <- scoreIndex.get(TimedScore(score, timestamp))
			s <- entrantIndex.get(r.value)
		} yield Entry(score, timestamp, r.value, r.rank, s.attachments)

	override def estimatedRank(score: Long): Long =
		scoreIndex.rank(TimedScore(score, 0))

	protected def better(newScore: TimedScore, oldScore: TimedScore) =
		ordering.lt(newScore, oldScore)

	override def post(post: Post, updateMode: UpdateMode = BestWins): Update = {
		import UpdateMode._
		val newScore = TimedScore(post.score, uniqueTimestamp, post.attachments)
		val (isBetter, oldEntry) = entrantIndex.get(post.entrant) match {
			case Some(oldScore) =>
				if (updateMode == LastWins || better(newScore, oldScore))
					(true, scoreIndex.remove(oldScore) map { o => Entry(o.key.score, o.key.timestamp, o.value, o.rank, o.key.attachments) })
				else
					(false, scoreIndex.get(oldScore) map { o => Entry(o.key.score, o.key.timestamp, o.value, o.rank, o.key.attachments) })
			case None => (true, None)
		}

		if (isBetter) {
			val newRow = scoreIndex.put(newScore, post.entrant)
			entrantIndex.put(post.entrant, newScore)
			Update(oldEntry, Some(Entry(newScore.score, newScore.timestamp, post.entrant, newRow.rank, post.attachments)))
		} else {
			Update(oldEntry, oldEntry)
		}
	}

	override def clear() = {
		entrantIndex.clear()
		scoreIndex.clear()
	}

	override def remove(entrant: String) = {
		val deleted =
			for {
				oldScore <- entrantIndex.remove(entrant)
				o <- scoreIndex.remove(oldScore)
			} yield Entry(o.key.score, o.key.timestamp, o.value, o.rank, o.key.attachments)

		Update(deleted, None)
	}

	private def updownrange(s: TimedScore, length: Int) =
		(scoreIndex.range(s, -length - 1).tail.reverse ++ scoreIndex.range(s, length + 1)) map { r =>
			Entry(r.key.score, r.key.timestamp, r.value, r.rank, r.key.attachments)
		}

	override def around(entrant: String, length: Int): Seq[Entry] =
		entrantIndex.get(entrant) match {
			case Some(s) => updownrange(s, length)
			case None => Seq()
		}

	override def around(score: Long, length: Int): Seq[Entry] =
		scoreIndex.range(TimedScore(score, 0), 1).headOption match {
			case Some(Row(s, value, rank)) => updownrange(s, length)
			case None => Seq()
		}

	override def at(rank: Long): Option[Entry] = page(rank, 1).headOption

	override def toString() =
		page(0, 10).toString()

	override def page(start: Long, length: Int): Seq[Entry] =
		for {
			r <- scoreIndex.page(start, length)
			s <- entrantIndex.get(r.value)
		} yield Entry(s.score, s.timestamp, r.value, r.rank, s.attachments)

	private[this] def uniqueTimestamp: Long = {
		val now = System.currentTimeMillis()
		if (now > _lastTime) {
			_lastTime = now
			_lastCount = 0
		} else {
			_lastCount += 1
		}
		_lastTime * Leaderboard.TimestampScale + _lastCount
	}
}