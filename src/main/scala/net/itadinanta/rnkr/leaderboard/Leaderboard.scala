package net.itadinanta.rnkr.leaderboard

import scalaz.ImmutableArray
import net.itadinanta.rnkr.tree.Tree
import net.itadinanta.rnkr.tree.Ordering
import scala.collection.mutable.Map
import net.itadinanta.rnkr.tree.Row

object UpdateMode extends Enumeration {
	type UpdateMode = Value
	val BestWins, LastWins, Delete = Value
}
import UpdateMode._
class Attachments(val data: ImmutableArray[Byte]) extends AnyVal
case class Entry(score: Long, timestamp: Long, entrant: String, rank: Long, attachments: Option[Attachments])
case class Post(score: Long, entrant: String, attachments: Option[Attachments], updateMode: UpdateMode)

trait Leaderboard {
	def size: Int
	def get(entrant: String*): Seq[Entry]
	def get(score: Long, timestamp: Long): Option[Entry]
	def at(rank: Long): Option[Entry]
	def estimatedRank(score: Long): Long
	def remove(entrant: String): Option[Entry]
	def post(post: Post): Entry
	def around(entrant: String, length: Int): Seq[Entry]
	def around(score: Long, length: Int): Seq[Entry]
	def page(start: Long, length: Int): Seq[Entry]
}

object Leaderboard {
	def apply(): Leaderboard = new LeaderboardTreeImpl()
}

class TimedScore private (val data: Array[Long]) extends AnyVal {
	def score = data(0)
	def timestamp = data(1)
}

object TimedScore {
	def apply(score: Long, timestamp: Long) = new TimedScore(Array(score, timestamp))
}

object TimedScoreOrdering extends Ordering[TimedScore] {
	def lt(a: TimedScore, b: TimedScore): Boolean = a.score < b.score || (a.score == b.score && a.timestamp < b.timestamp)
}

class LeaderboardTreeImpl extends Leaderboard {
	val scoreIndex = Tree[TimedScore, String](TimedScoreOrdering)
	val entrantIndex = Map[String, (TimedScore, Option[Attachments])]()
	val attachments = Map[String, Attachments]()

	private[this] var _lastTime: Long = System.currentTimeMillis
	private[this] var _lastCount: Long = 0

	def size: Int = scoreIndex.size

	def get(entrants: String*) =
		for {
			e <- entrants
			(s, a) <- entrantIndex.get(e)
			r <- scoreIndex.get(s)
		} yield Entry(s.score, s.timestamp, e, r.rank, a)

	def get(score: Long, timestamp: Long) =
		for {
			r <- scoreIndex.get(TimedScore(score, timestamp))
			(_, a) <- entrantIndex.get(r.value)
		} yield Entry(score, timestamp, r.value, r.rank, a)

	def estimatedRank(score: Long): Long =
		scoreIndex.rank(TimedScore(score, 0))

	def remove(entrant: String): Option[Entry] =
		entrantIndex.remove(entrant) flatMap {
			case (s, a) => scoreIndex.remove(s) map {
				r => Entry(s.score, s.timestamp, r.value, r.rank, a)
			}
		}

	def post(post: Post): Entry =
		Entry(post.score, uniqueTimestamp, post.entrant, 0, post.attachments)

	private def updownrange(s: TimedScore, length: Int) =
		(scoreIndex.range(s, -length).reverse ++ scoreIndex.range(s, length + 1)) map { r =>
			Entry(r.key.score, r.key.timestamp, r.value, r.rank, entrantIndex.get(r.value) flatMap { _._2 })
		}

	def around(entrant: String, length: Int): Seq[Entry] =
		entrantIndex.get(entrant) match {
			case Some((s, _)) => updownrange(s, length)
			case None => Seq()
		}

	def around(score: Long, length: Int): Seq[Entry] =
		scoreIndex.range(TimedScore(score, 0), 1).headOption match {
			case Some(Row(s, value, rank)) => updownrange(s, length)
			case None => Seq()
		}

	def at(rank: Long): Option[Entry] = page(rank, 1).headOption

	def page(start: Long, length: Int): Seq[Entry] =
		for {
			r <- scoreIndex.page(start, length)
			(s, a) <- entrantIndex.get(r.value)
		} yield Entry(s.score, s.timestamp, r.value, r.rank, a)

	private[this] def uniqueTimestamp = {
		val now = System.currentTimeMillis
		if (now > _lastTime) {
			_lastTime = now
			_lastCount = 0
		} else {
			_lastCount += 1
		}
		_lastTime * 10000000 + _lastCount
	}
}