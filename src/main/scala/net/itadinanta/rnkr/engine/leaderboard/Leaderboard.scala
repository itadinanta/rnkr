package net.itadinanta.rnkr.engine.leaderboard

import scala.concurrent.Future
import scalaz.ImmutableArray

object UpdateMode extends Enumeration {
	type UpdateMode = Value
	val BestWins, LastWins = Value
}

class Attachments(val data: ImmutableArray[Byte]) extends AnyVal;

object Attachments {
	def apply(s: String): Attachments = new Attachments(ImmutableArray.fromArray(s.getBytes("UTF8")))
	def apply(s: Option[String]): Option[Attachments] = for { v <- s } yield Attachments(v)
}

case class Entry(score: Long, timestamp: Long, entrant: String, rank: Long, attachments: Option[Attachments])
case class Post(score: Long, entrant: String, attachments: Option[Attachments])
case class Update(oldEntry: Option[Entry], newEntry: Option[Entry])

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

	def post(post: Post, updateMode: UpdateMode = BestWins): Future[Update]
	def remove(entrant: String): Future[Update]
	def clear(): Future[Int]
}