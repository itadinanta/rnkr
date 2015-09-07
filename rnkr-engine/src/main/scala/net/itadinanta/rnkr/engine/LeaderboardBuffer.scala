package net.itadinanta.rnkr.engine

import scala.collection.mutable.Map
import akka.util.ByteString
import net.itadinanta.rnkr.backend.Replay
import net.itadinanta.rnkr.core.tree.Ordering
import net.itadinanta.rnkr.core.tree.RankedTreeMap
import net.itadinanta.rnkr.core.tree.Row
import net.itadinanta.rnkr.backend.ReplayMode
import Leaderboard._
import Leaderboard.UpdateMode._

trait LeaderboardBuffer {
	def size: Int
	def isEmpty: Boolean
	def lookup(entrant: String*): Seq[Entry]
	def get(score: Long, timestamp: Long): Option[Entry]
	def at(rank: Long): Option[Entry]
	def estimatedRank(score: Long): Long
	def nearby(entrant: String, length: Int): Seq[Entry]
	def around(score: Long, length: Int): Seq[Entry]
	def page(start: Long, length: Int): Seq[Entry]

	def export(): Snapshot

	def remove(entrant: String): Update
	def post(post: Post, updateMode: UpdateMode = BestWins): Update
	def clear(): Update

	def replay(entries: Iterable[Replay]): Iterable[Update]
	def append(entries: Iterable[Entry]): Iterable[Update]
}

object LeaderboardBuffer {
	val TIMESTAMP_SCALE = 1000000L

	trait Factory {
		def build(): LeaderboardBuffer = new LeaderboardTreeImpl()
	}
}

protected case class TimedScore(
		val score: Long,
		val timestamp: Long,
		val attachments: Option[Attachments] = None) {
	override def hashCode = ((31 * score) + timestamp).intValue
	override def equals(o: Any) = o match {
		case t: TimedScore => t.score == score && t.timestamp == timestamp
	}
}

protected object TimedScoreOrdering extends Ordering[TimedScore] {
	override def lt(a: TimedScore, b: TimedScore): Boolean =
		a.score < b.score || (a.score == b.score && a.timestamp < b.timestamp)
}

private class LeaderboardTreeImpl extends LeaderboardBuffer {
	val ordering = TimedScoreOrdering
	val scoreIndex = RankedTreeMap[TimedScore, ByteString](ordering)
	val entrantIndex = Map[ByteString, TimedScore]()

	def asByteString(s: String): ByteString = ByteString(s)
	def asString(s: ByteString): String = s.utf8String

	private[this] var _lastTime: Long = System.currentTimeMillis
	private[this] var _lastCount: Long = 0

	override def size = scoreIndex.size
	override def isEmpty = scoreIndex.isEmpty

	override def lookup(entrants: String*) =
		for {
			e <- entrants
			s <- entrantIndex.get(asByteString(e))
			r <- scoreIndex.get(s)
		} yield Entry(s.score, s.timestamp, e, r.rank, s.attachments)

	override def get(score: Long, timestamp: Long) =
		for {
			r <- scoreIndex.get(TimedScore(score, timestamp))
			s <- entrantIndex.get(r.value)
		} yield Entry(score, timestamp, asString(r.value), r.rank, s.attachments)

	override def estimatedRank(score: Long): Long =
		scoreIndex.rank(TimedScore(score, 0))

	protected def better(newScore: TimedScore, oldScore: TimedScore) =
		ordering.lt(newScore, oldScore)

	override def post(p: Post, updateMode: UpdateMode = BestWins): Update =
		post(TimedScore(p.score, timestamp, p.attachments), p.entrant, updateMode)

	private[this] def post(newScore: TimedScore, entrant: String, updateMode: UpdateMode): Update = {
		import UpdateMode._
		val entrantKey = asByteString(entrant)
		val (isBetter, oldEntry) = entrantIndex.get(entrantKey) match {
			case Some(oldScore) =>
				if (updateMode == LastWins || better(newScore, oldScore))
					(true, scoreIndex.remove(oldScore) map { o =>
						Entry(o.key.score, o.key.timestamp, asString(o.value), o.rank, o.key.attachments)
					})
				else
					(false, scoreIndex.get(oldScore) map { o =>
						Entry(o.key.score, o.key.timestamp, asString(o.value), o.rank, o.key.attachments)
					})
			case None => (true, None)
		}

		if (isBetter) {
			val newRow = scoreIndex.put(newScore, entrantKey)
			entrantIndex.put(entrantKey, newScore)
			Update(newScore.timestamp,
				true,
				oldEntry,
				Some(Entry(newScore.score, newScore.timestamp, entrant, newRow.rank, newScore.attachments)))
		} else {
			Update(newScore.timestamp,
				false,
				oldEntry,
				oldEntry)
		}
	}

	override def clear() = {
		if (entrantIndex.isEmpty) {
			Update(timestamp(), false, None, None)
		} else {
			entrantIndex.clear()
			scoreIndex.clear()
			Update(timestamp(), true, None, None)
		}
	}

	override def remove(entrant: String) = {
		val entrantKey = asByteString(entrant)
		val deleted =
			for {
				oldScore <- entrantIndex.remove(entrantKey)
				o <- scoreIndex.remove(oldScore)
			} yield Entry(o.key.score, o.key.timestamp, asString(o.value), o.rank, o.key.attachments)

		Update(timestamp(), !deleted.isEmpty, deleted, None)
	}

	private def updownrange(s: TimedScore, length: Int) =
		(scoreIndex.range(s, -length - 1).tail.reverse ++ scoreIndex.range(s, length + 1)) map { r =>
			Entry(r.key.score, r.key.timestamp, asString(r.value), r.rank, r.key.attachments)
		}

	override def nearby(entrant: String, length: Int): Seq[Entry] =
		entrantIndex.get(asByteString(entrant)) match {
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
		} yield Entry(s.score, s.timestamp, asString(r.value), r.rank, s.attachments)

	override def export() = Snapshot(timestamp(),
		for {
			((k, value), rank) <- scoreIndex.entries().zipWithIndex
			s <- entrantIndex.get(value)
		} yield Entry(s.score, s.timestamp, asString(value), rank + 1, s.attachments))

	override def replay(entries: Iterable[Replay]): Iterable[Update] = entries map {
		_ match {
			case Replay(ReplayMode.LastWins, score, timestamp, entrant, attachments) =>
				post(TimedScore(score, timestamp, attachments), entrant, UpdateMode.LastWins)
			case Replay(ReplayMode.BestWins, score, timestamp, entrant, attachments) =>
				post(TimedScore(score, timestamp, attachments), entrant, UpdateMode.BestWins)
			case Replay(ReplayMode.Delete, _, _, entrant, _) =>
				remove(entrant)
			case Replay(ReplayMode.Clear, timestamp, _, _, _) =>
				clear()
		}
	}

	override def append(entries: Iterable[Entry]): Iterable[Update] = entries map { e =>
		val key = TimedScore(e.score, e.timestamp, e.attachments)
		val value = asByteString(e.entrant)
		scoreIndex.append(key, value)
		entrantIndex.put(value, key)
		Update(e.timestamp, true, None, Some(e))
	}

	private[this] def timestamp(): Long = {
		val now = System.currentTimeMillis()
		if (now > _lastTime) {
			_lastTime = now
			_lastCount = 0
		} else {
			_lastCount += 1
		}
		_lastTime * LeaderboardBuffer.TIMESTAMP_SCALE + _lastCount
	}
}