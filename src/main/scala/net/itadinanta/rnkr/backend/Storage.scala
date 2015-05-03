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

object ReplayMode extends Enumeration {
	type ReplayMode = Value
	val BestWins, LastWins, Delete, Clear = Value
	def apply(updateMode: UpdateMode) = updateMode match {
		case UpdateMode.BestWins => ReplayMode.BestWins
		case UpdateMode.LastWins => ReplayMode.LastWins
	}
}
case class Watermark(watermark: Long, pages: Int)
case class Replay(replayMode: ReplayMode.ReplayMode, score: Long, timestamp: Long, entrant: String, attachments: Option[Attachments])

case class Load(watermark: Long, walLength: Int)
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
}

object Reader {
	def props(cluster: Cassandra, id: String, leaderboard: LeaderboardBuffer) = Props(new Reader(cluster, id, leaderboard))
}

class Reader(override val cluster: Cassandra, val id: String, val leaderboard: LeaderboardBuffer) extends Storage with Logging {
	import QueryBuilder._
	case class Page(page: Int, entries: Iterable[Entry])
	case class PageReadRequest(page: Int)

	lazy val readLastWatermarkStatement = session.prepare(
		select("watermark", "pages")
			.from("watermarks")
			.where(QueryBuilder.eq("id", bindMarker()))
			.orderBy(desc("watermark")).limit(1))
		.setConsistencyLevel(ConsistencyLevel.ONE)

	def loadWatermark() =
		session.executeAsync(readLastWatermarkStatement.bind(id)) map {
			_.headOption match {
				case Some(data) => Watermark(data.getLong("watermark"), data.getInt("pages"))
				case None => Watermark(0, 0)
			}
		}

	lazy val readPageStatement = session.prepare(
		select("scoredata")
			.from("pages")
			.where(QueryBuilder.eq("id", bindMarker()))
			.and(QueryBuilder.eq("watermark", bindMarker()))
			.and(QueryBuilder.eq("page", bindMarker())))
		.setConsistencyLevel(ConsistencyLevel.ONE)

	def loadPages(watermark: Long, pages: Int) = {

		def loadPage(watermark: Long, page: Int) =
			session.executeAsync(readPageStatement.bind(id, JLong.valueOf(watermark), Integer.valueOf(page))) map { resultSet =>
				val entries = resultSet.flatMap { resultItem =>
					resultItem.getString("scoredata").lines map { line =>
						val s = line.split(Storage.CSV_SEPARATOR)
						Entry(decodeScore(s(0)), s(1).toLong, s(2), s(3).toLong, if (s.length < 5) None else decodeAttachments(s(4)))
					}
				}
				debug(s"Loaded ${entries.size} for page ${page}")
				Page(page, entries)
			}

		def append(l: List[Future[Page]]): Future[Int] =
			if (l.isEmpty)
				Future { pages }
			else
				l.head flatMap { p =>
					leaderboard.append(p.entries)
					append(l.tail)
				}

		append((0 to pages - 1).toList.map { loadPage(watermark, _) })
	}

	lazy val readWalStatement = session.prepare(
		select("seq", "scoredata")
			.from("wal")
			.where(QueryBuilder.eq("id", bindMarker()))
			.and(QueryBuilder.gte("watermark", bindMarker())))
		.setConsistencyLevel(ConsistencyLevel.ONE)

	def decodeAttachments(s: String) = s match {
		case "" => None
		case o => Some(Attachments(o))
	}

	def decodeScore(s: String) = s match {
		case "" => Storage.TOMBSTONE
		case o => o.toLong
	}

	def replayWal(watermark: Long) = {
		var walLength = 0
		session.executeAsync(readWalStatement.bind(id, JLong.valueOf(watermark))) map { results =>
			leaderboard.replay(
				results.map { r =>
					val timestamp = r.getLong("seq")
					val s = r.getString("scoredata").split(Storage.CSV_SEPARATOR)
					walLength += 1
					Replay(ReplayMode(s(0).toInt), decodeScore(s(1)), timestamp, s(2), if (s.size < 4) None else decodeAttachments(s(3)))
				})
		} map {
			_ => Load(watermark, walLength)
		}
	}

	def receive = {
		case Load =>
			val src = sender()
			loadWatermark() map { w =>
				loadPages(w.watermark, w.pages) map { r => // always true
					replayWal(w.watermark) onSuccess {
						case result =>
							debug(s"Load complete for ${id}")
							src ! result
					}
				}
			}
	}
}

object Writer {
	val PAGESIZE = 1000
	def props(cluster: Cassandra, id: String, watermark: Long) = Props(new Writer(cluster, id, watermark))
}

class Writer(override val cluster: Cassandra, val id: String, initialWatermark: Long) extends Storage {
	import QueryBuilder._
	var watermark = initialWatermark
	lazy val storeWalStatement = session.prepare(QueryBuilder.insertInto("wal")
		.value("id", bindMarker())
		.value("watermark", bindMarker())
		.value("seq", bindMarker())
		.value("scoredata", bindMarker()))
		.setConsistencyLevel(ConsistencyLevel.ONE)

	// TODO: attachments need printf escaping
	def encode(attachments: Option[Attachments]) = attachments map { s => new String(s.data.toArray, "UTF8") } getOrElse ("")
	def encode(score: Long) = if (score == Storage.TOMBSTONE) "" else score.toString

	def storeWal(mode: ReplayMode.ReplayMode, timestamp: Long, w: Post) = {
		val seq = JLong.valueOf(timestamp)
		val wm = JLong.valueOf(watermark)
		val scoredata = Seq(mode.id, encode(w.score), w.entrant, encode(w.attachments)).mkString(Storage.CSV_SEPARATOR)
		val statement = storeWalStatement.bind(id, wm, seq, scoredata)
		session.executeAsync(statement) map { _ => w }
	}

	lazy val storePageStatement = session.prepare(
		insertInto("pages")
			.value("id", bindMarker())
			.value("watermark", bindMarker())
			.value("page", bindMarker())
			.value("scoredata", bindMarker()))
		.setConsistencyLevel(ConsistencyLevel.ONE)

	def storeRows(watermark: Long, page: Int, rows: Seq[Entry]): Future[Int] = {
		val scoredata = rows map { row =>
			Seq(encode(row.score), row.timestamp, row.entrant, row.rank, encode(row.attachments)).mkString(Storage.CSV_SEPARATOR)
		} mkString ("\n")
		val statement = storePageStatement.bind(id, JLong.valueOf(watermark), Integer.valueOf(page), scoredata)
		session.executeAsync(statement) map { _ => page }
	}

	lazy val storeWatermarks = session.prepare(
		insertInto("watermarks")
			.value("id", bindMarker())
			.value("watermark", bindMarker())
			.value("pages", bindMarker()))
		.setConsistencyLevel(ConsistencyLevel.ONE)

	def storeWatermark(watermark: Long, pages: Int) = session.executeAsync(storeWatermarks.bind(id, JLong.valueOf(watermark), Integer.valueOf(pages))) map {
		_ => Watermark(watermark, pages)
	}

	def compact(watermark: Watermark) = {
		watermark
	}

	case class PageWriteRequest(page: Int)

	def receive = {
		case WriteAheadLog(mode, seq, w) =>
			val src = sender()
			storeWal(mode, seq, w) foreach { src ! WriteAheadLog(mode, seq, _) }

		case Save(snapshot) =>
			this.watermark = snapshot.timestamp
			val src = sender()
			Future.sequence(snapshot.entries.grouped(Writer.PAGESIZE).zipWithIndex.map {
				case (pageItems, pageIndex) => storeRows(snapshot.timestamp, pageIndex, pageItems)
			}) map { pages =>
				storeWatermark(snapshot.timestamp, pages.size) map {
					watermark =>
						src ! Save
						// this can be done in background
						compact(watermark)
				}
			}
	}
}