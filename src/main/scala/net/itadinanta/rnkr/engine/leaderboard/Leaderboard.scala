package net.itadinanta.rnkr.engine.leaderboard

import scala.concurrent.Future
import scalaz.ImmutableArray

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
case class Update(timestamp: Long, oldEntry: Option[Entry], newEntry: Option[Entry])
case class Snapshot(timestamp: Long, entries: Seq[Entry])

trait Leaderboard {
	import UpdateMode._
	def size: Future[Int]
	def isEmpty: Future[Boolean]
	def get(entrant: String*): Future[Seq[Entry]]
	def get(score: Long, timestamp: Long): Future[Option[Entry]]
	def at(rank: Long): Future[Option[Entry]]
	def estimatedRank(score: Long): Future[Long]
	def around(entrant: String, length: Int): Future[Seq[Entry]]
	def around(score: Long, length: Int): Future[Seq[Entry]]
	def page(start: Long, length: Int): Future[Seq[Entry]]
	
	def export(): Future[Snapshot]

	def post(post: Post, updateMode: UpdateMode = BestWins): Future[Update]
	def remove(entrant: String): Future[Update]
	def clear(): Future[Update]
}

abstract class LeaderboardDecorator(protected[this] val target: Leaderboard) extends Leaderboard {
	import UpdateMode._
	override def size = target.size
	override def isEmpty = target.isEmpty
	override def get(entrant: String*) = target.get(entrant: _*)
	override def get(score: Long, timestamp: Long) = target.get(score, timestamp)
	override def at(rank: Long) = target.at(rank)
	override def estimatedRank(score: Long) = target.estimatedRank(score)
	override def around(entrant: String, length: Int) = target.around(entrant, length)
	override def around(score: Long, length: Int) = target.around(score, length)
	override def page(start: Long, length: Int) = target.page(start, length)
	override def export() = target.export()

	override def post(post: Post, updateMode: UpdateMode = BestWins) = target.post(post, updateMode)
	override def remove(entrant: String) = target.remove(entrant)
	override def clear() = target.clear()
}
