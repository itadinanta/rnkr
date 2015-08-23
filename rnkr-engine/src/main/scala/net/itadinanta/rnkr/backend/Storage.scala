package net.itadinanta.rnkr.backend

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.{ Row => CassandraRow }
import net.itadinanta.rnkr.core.tree.Row
import akka.actor.ActorRef
import akka.pattern.pipe
import akka.actor.Actor
import scala.concurrent.Future
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.BatchStatement
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.ResultSetFuture
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
import com.datastax.driver.core.Session
import akka.actor.PoisonPill

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

object Storage {
	val CSV_SEPARATOR = ";"
	val TOMBSTONE = Long.MinValue
	def tombstone(entrant: String) = Post(TOMBSTONE, entrant, None)
	def tombstone() = Post(TOMBSTONE, "", None)
}

trait Storage extends Actor {

	val cluster: Cassandra
	// TODO: share session and statements
	val session = cluster.cluster.connect("akkacassandra")
	implicit lazy val executionContext = context.system.dispatcher

	implicit def akkaFuture(arg: ResultSetFuture): Future[ResultSet] = {
		val p = Promise[ResultSet]
		Futures.addCallback(arg, new FutureCallback[ResultSet] {
			def onSuccess(r: ResultSet) { p.success(r) }
			def onFailure(r: Throwable) { p.failure(r) }
		})
		return p.future
	}

	override def postStop() {
		session.close
	}
}

object Reader {
	def props(cluster: Cassandra, id: String, leaderboard: LeaderboardBuffer) = Props(new Reader(cluster, id, leaderboard))
}

trait ReaderStatements {
	import QueryBuilder._
	val session: Session
	val consistencyLevel = ConsistencyLevel.ONE

	lazy val readLastWatermarkStatement = session.prepare(
		select("watermark", "pages")
			.from("watermarks")
			.where(QueryBuilder.eq("id", bindMarker()))
			.orderBy(desc("watermark")).limit(1))
		.setConsistencyLevel(consistencyLevel)

	lazy val readPageStatement = session.prepare(
		select("scoredata")
			.from("pages")
			.where(QueryBuilder.eq("id", bindMarker()))
			.and(QueryBuilder.eq("watermark", bindMarker()))
			.and(QueryBuilder.eq("page", bindMarker())))
		.setConsistencyLevel(consistencyLevel)

	lazy val readWalStatement = session.prepare(
		select("seq", "scoredata")
			.from("wal")
			.where(QueryBuilder.eq("id", bindMarker()))
			.and(QueryBuilder.eq("watermark", bindMarker())))
		.setConsistencyLevel(consistencyLevel)

	lazy val readMetadataStatement = session.prepare(
		select("sorting", "encoding", "comment", "page_size", "wal_size_limit", "wal_time_limit")
			.from("metadata")
			.where(QueryBuilder.eq("id", bindMarker())))
		.setConsistencyLevel(consistencyLevel)
}

class Reader(override val cluster: Cassandra, val id: String, val leaderboard: LeaderboardBuffer)
		extends Storage
		with ReaderStatements
		with Logging {

	case class Page(page: Int, entries: Iterable[Entry])
	case class PageReadRequest(page: Int)

	def loadWatermark() =
		for { wmRow <- session.executeAsync(readLastWatermarkStatement.bind(id)) } yield wmRow.headOption match {
			case Some(data) => Watermark(data.getLong("watermark"), data.getInt("pages"))
			case None => Watermark(0, 0)
		}

	def loadPages(watermark: Long, pages: Int) = {
		def loadPage(watermark: Long, page: Int) =
			for {
				resultSet <- session.executeAsync(readPageStatement.bind(id, JLong.valueOf(watermark), Integer.valueOf(page)))
				entries = for {
					row <- resultSet
					line <- row.getString("scoredata").lines
					s = line.split(Storage.CSV_SEPARATOR)
				} yield Entry(decodeScore(s(0)), s(1).toLong, s(2), s(3).toLong, if (s.length < 5) None else decodeAttachments(s(4)))
			} yield Page(page, entries)

		def append(l: List[Future[Page]]): Future[Int] =
			if (l.isEmpty)
				Future.successful(pages)
			else l.head flatMap { p =>
				leaderboard.append(p.entries)
				append(l.tail)
			}

		debug(s"Loading of ${pages} pages started at watermark ${watermark} for ${id}")
		val job = append(for { i <- (0 to pages - 1).toList } yield loadPage(watermark, i))
		job onSuccess {
			case _ => debug(s"Loading completed of ${pages} pages for up to ${leaderboard.size} entries for ${id}")
		}
		job
	}

	def decodeAttachments(s: String) = s match {
		case "" => None
		case o => Some(Attachments(o))
	}

	def decodeScore(s: String) = s match {
		case "" => Storage.TOMBSTONE
		case o => o.toLong
	}

	def replayWal(watermark: Long) =
		for { results <- session.executeAsync(readWalStatement.bind(id, JLong.valueOf(watermark))) } yield {
			debug(s"Replaying wal for ${id}")
			var walLength = 0
			val log = for { r <- results } yield {
				val timestamp = r.getLong("seq")
				val s = r.getString("scoredata").split(Storage.CSV_SEPARATOR)
				walLength += 1
				Replay(ReplayMode(s(0).toInt),
					if (s.size > 1) decodeScore(s(1)) else Storage.TOMBSTONE,
					timestamp,
					if (s.size > 2) s(2) else "",
					if (s.size > 3) decodeAttachments(s(3)) else None)
			}
			leaderboard.replay(log)
			debug(s"Replayed ${walLength} posts from wal")
			walLength
		}

	def loadMetadata() =
		for { md <- session.executeAsync(readMetadataStatement.bind(id)) } yield {
			for { r <- md.headOption } yield {
				val m = Metadata(r.getString("comment"),
					r.getInt("page_size"),
					r.getInt("wal_size_limit"),
					r.getLong("wal_time_limit"))
				debug(s"Loaded stored metadata ${m} for ${id}")
				m
			}
		} getOrElse {
			val m = Metadata()
			debug(s"Loaded default metadata ${m} for ${id}")
			m
		}

	override def receive = {
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

object Writer {
	def props(cluster: Cassandra, id: String, watermark: Long, metadata: Metadata) = Props(new Writer(cluster, id, watermark, metadata))
}

trait WriterStatements {
	import QueryBuilder._
	val session: Session
	val consistencyLevel = ConsistencyLevel.ONE

	lazy val storeWalStatement = session.prepare(
		insertInto("wal")
			.value("id", bindMarker())
			.value("watermark", bindMarker())
			.value("seq", bindMarker())
			.value("scoredata", bindMarker()))
		.setConsistencyLevel(consistencyLevel)

	lazy val storePageStatement = session.prepare(
		insertInto("pages")
			.value("id", bindMarker())
			.value("watermark", bindMarker())
			.value("page", bindMarker())
			.value("scoredata", bindMarker()))
		.setConsistencyLevel(consistencyLevel)

	lazy val storeWatermarks = session.prepare(
		insertInto("watermarks")
			.value("id", bindMarker())
			.value("watermark", bindMarker())
			.value("pages", bindMarker()))
		.setConsistencyLevel(consistencyLevel)

	lazy val readOldWatermarksStatement = session.prepare(
		select("watermark", "pages")
			.from("watermarks")
			.where(QueryBuilder.eq("id", bindMarker()))
			.and(lt("watermark", bindMarker())))
		.setConsistencyLevel(consistencyLevel)

	lazy val readOldWalStatement = session.prepare(
		select("watermark", "seq")
			.from("wal")
			.where(QueryBuilder.eq("id", bindMarker()))
			.and(lt("watermark", bindMarker())))
		.setConsistencyLevel(consistencyLevel)

	lazy val deleteOldWatermarksStatement = session.prepare(
		delete().all()
			.from("watermarks")
			.where(QueryBuilder.eq("id", bindMarker()))
			.and(QueryBuilder.eq("watermark", bindMarker())))
		.setConsistencyLevel(consistencyLevel)

	lazy val deleteOldPagesStatement = session.prepare(
		delete().all()
			.from("pages")
			.where(QueryBuilder.eq("id", bindMarker()))
			.and(QueryBuilder.eq("watermark", bindMarker()))
			.and(QueryBuilder.eq("page", bindMarker())))
		.setConsistencyLevel(consistencyLevel)

	lazy val deleteOldWalStatement = session.prepare(
		delete().all()
			.from("wal")
			.where(QueryBuilder.eq("id", bindMarker()))
			.and(QueryBuilder.eq("watermark", bindMarker())))
		.setConsistencyLevel(consistencyLevel)
}

class Writer(override val cluster: Cassandra, val id: String, initialWatermark: Long, val metadata: Metadata)
		extends Storage
		with WriterStatements
		with Logging {

	import QueryBuilder._

	// TODO: attachments need printf escaping
	def encode(attachments: Option[Attachments]) = attachments map { s => new String(s.data.toArray, "UTF8") } getOrElse ""
	def encode(score: Long) = if (score == Storage.TOMBSTONE) "" else score.toString

	def storeWal(mode: ReplayMode.ReplayMode, timestamp: Long, watermark: Long, w: Post) = {
		val seq = JLong.valueOf(timestamp)
		val wm = JLong.valueOf(watermark)
		val scoredata = Seq(mode.id, encode(w.score), w.entrant, encode(w.attachments)) mkString Storage.CSV_SEPARATOR
		for { _ <- session.executeAsync(storeWalStatement.bind(id, wm, seq, scoredata)) } yield w
	}

	def storeSnapshot(watermark: Long, snapshot: Snapshot) = {
		def storeRows(page: Int, rows: Seq[Entry]): Future[Int] = {
			val scorerows = for { row <- rows } yield Seq(encode(row.score), row.timestamp, row.entrant, row.rank, encode(row.attachments)) mkString Storage.CSV_SEPARATOR
			val scoredata = scorerows mkString "\n"
			val statement = storePageStatement.bind(id, JLong.valueOf(watermark), Integer.valueOf(page), scoredata)
			for { _ <- session.executeAsync(statement) } yield page
		}

		Future.sequence {
			for ((pageItems, pageIndex) <- snapshot.entries.grouped(metadata.pageSize).zipWithIndex)
				yield storeRows(pageIndex, pageItems)
		}
	}
	def storeWatermark(watermark: Long, pages: Int) =
		for { _ <- session.executeAsync(storeWatermarks.bind(id, JLong.valueOf(watermark), Integer.valueOf(pages))) }
			yield Watermark(watermark, pages)

	def compact(watermark: Watermark) = {
		debug(s"Deleting up to ${watermark}")
		val watermarkFuture = for { row <- session.executeAsync(readOldWatermarksStatement.bind(id, JLong.valueOf(watermark.watermark))) } yield {
			val watermarks = row map { wmRow => Watermark(wmRow.getLong("watermark"), wmRow.getInt("pages")) }
			if (!watermarks.isEmpty) {
				val batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
				debug(s"Found ${watermarks}")
				for (wm <- watermarks; page <- 0 to wm.pages - 1) batch.add(
					deleteOldPagesStatement.bind(id, JLong.valueOf(wm.watermark), Integer.valueOf(page)))
				for (wm <- watermarks) batch.add(
					deleteOldWalStatement.bind(id, JLong.valueOf(wm.watermark)))
				for (wm <- watermarks) batch.add(
					deleteOldWatermarksStatement.bind(id, JLong.valueOf(wm.watermark)))

				session.executeAsync(batch) onFailure { case e => error(e) }
			}
		}
		watermarkFuture onFailure { case e => error(e) }
	}

	var watermark = initialWatermark
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