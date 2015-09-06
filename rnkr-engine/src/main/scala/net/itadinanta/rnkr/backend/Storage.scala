package net.itadinanta.rnkr.backend

import net.itadinanta.rnkr.core.tree.Row
import akka.actor.ActorRef
import akka.pattern.pipe
import akka.actor.Actor
import scala.concurrent.Future
import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.FutureCallback
import scala.concurrent.Promise
import scala.collection.JavaConversions._
import com.datastax.driver.core.querybuilder.QueryBuilder
import akka.actor.Props
import net.itadinanta.rnkr.engine.leaderboard.LeaderboardBuffer
import net.itadinanta.rnkr.engine.leaderboard.LeaderboardArbiter
import net.itadinanta.rnkr.core.arbiter.Arbiter
import net.itadinanta.rnkr.engine.leaderboard.Entry
import com.google.common.io.BaseEncoding
import net.itadinanta.rnkr.engine.leaderboard.Post
import net.itadinanta.rnkr.engine.leaderboard.Attachments
import net.itadinanta.rnkr.engine.leaderboard.UpdateMode._
import java.nio.ByteBuffer
import net.itadinanta.rnkr.engine.leaderboard.UpdateMode
import net.itadinanta.rnkr.engine.leaderboard.Update
import net.itadinanta.rnkr.engine.leaderboard.Snapshot
import java.lang.{ Long => JLong }
import grizzled.slf4j.Logging
import scala.annotation.tailrec
import akka.actor.PoisonPill
import scala.concurrent.ExecutionContext

object ReplayMode extends Enumeration {
	type ReplayMode = Value
	val BestWins, LastWins, Delete, Clear = Value
	def apply(updateMode: UpdateMode) = updateMode match {
		case UpdateMode.BestWins => ReplayMode.BestWins
		case UpdateMode.LastWins => ReplayMode.LastWins
	}
}
case class Watermark(watermark: Long, pages: Int)
case class Metadata(val comment: String = "", val pageSize: Int = 2500, val walSizeLimit: Int = 10000, val walTimeLimit: Long = 1800000L)
case class Replay(replayMode: ReplayMode.ReplayMode, score: Long, timestamp: Long, entrant: String, attachments: Option[Attachments])

case class Load(watermark: Long, walLength: Int, metadata: Metadata)
case class Save(snapshot: Snapshot)
case class WriteAheadLog(mode: ReplayMode.ReplayMode, seq: Long, w: Post)
case class Flush(snapshot: Snapshot)

trait Storage extends Actor {
	val datastore: Datastore
}

object Storage {
	val CSV_SEPARATOR = ";"
	val TOMBSTONE = Long.MinValue
	def tombstone(entrant: String) = Post(TOMBSTONE, entrant, None)
	def tombstone() = Post(TOMBSTONE, "", None)

	trait Reader extends Storage with Logging {
		val id: String
		val leaderboard: LeaderboardBuffer
		implicit val executionContext: ExecutionContext

		case class Page(page: Int, entries: Iterable[Entry])
		case class PageReadRequest(page: Int)

		def loadWatermark(): Future[Watermark]
		def loadPage(watermark: Long, page: Int): Future[Page]
		def append(l: List[Future[Page]], pageCount: Int): Future[Int]
		def replayWal(watermark: Long): Future[Int]
		def loadMetadata(): Future[Metadata]

		final def loadPages(watermark: Long, pageCount: Int) = {
			debug(s"Loading of ${pageCount} pages started at watermark ${watermark} for ${id}")
			val job = append(for { i <- (0 to pageCount - 1).toList } yield loadPage(watermark, i), pageCount)
			job onSuccess {
				case _ => debug(s"Loading completed of ${pageCount} pages for up to ${leaderboard.size} entries for ${id}")
			}
			job
		}

		final def decodeAttachments(s: String) = s match {
			case "" => None
			case o => Some(Attachments(o))
		}

		final def decodeScore(s: String) = s match {
			case "" => Storage.TOMBSTONE
			case o => o.toLong
		}

		final override def receive = {
			case Load =>
				val src = sender()
				for {
					w <- loadWatermark()
					pagesRead <- loadPages(w.watermark, w.pages)
					walLength <- replayWal(w.watermark)
					metadata <- loadMetadata()
				} yield {
					debug(s"Load complete for ${id}")
					src ! Load(w.watermark, walLength, metadata)
					self ! PoisonPill
				}
		}
	}

	trait Writer extends Storage with Logging {
		val id: String
		val initialWatermark: Long
		val metadata: Metadata
		var watermark = initialWatermark
		implicit val executionContext: ExecutionContext

		def storeWal(mode: ReplayMode.ReplayMode, timestamp: Long, watermark: Long, w: Post): Future[Post]
		def storeRows(page: Int, rows: Seq[Entry]): Future[Int]
		def storeWatermark(watermark: Long, pages: Int): Future[Watermark]
		def compact(watermark: Watermark): Unit

		// TODO: attachments need printf escaping
		def encode(attachments: Option[Attachments]) = attachments map { s => new String(s.data.toArray, "UTF8") } getOrElse ""
		def encode(score: Long) = if (score == Storage.TOMBSTONE) "" else score.toString

		final def storeSnapshot(watermark: Long, snapshot: Snapshot): Future[Iterator[Int]] = Future.sequence {
			for ((pageItems, pageIndex) <- snapshot.entries.grouped(metadata.pageSize).zipWithIndex)
				yield storeRows(pageIndex, pageItems)
		}

		def receive = {
			case WriteAheadLog(mode, seq, action) =>
				val src = sender()
				for (savedLine <- storeWal(mode, seq, watermark, action)) yield src ! WriteAheadLog(mode, seq, savedLine)

			case Save(snapshot) =>
				this.watermark = snapshot.timestamp
				val src = sender()
				for {
					savedPages <- storeSnapshot(watermark, snapshot)
					watermark <- storeWatermark(watermark, savedPages.size)
				} yield {
					src ! Save
					compact(watermark)
				}
		}
	}
}
