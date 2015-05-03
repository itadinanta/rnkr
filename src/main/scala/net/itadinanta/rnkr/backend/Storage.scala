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

object ReplayMode extends Enumeration {
	type ReplayMode = Value
	val BestWins, LastWins, Delete, Clear = Value
	val TOMBSTONE = Long.MinValue
	def apply(updateMode: UpdateMode) = updateMode match {
		case UpdateMode.BestWins => ReplayMode.BestWins
		case UpdateMode.LastWins => ReplayMode.LastWins
	}
}
case class Replay(replayMode: ReplayMode.ReplayMode, score: Long, timestamp: Long, entrant: String, attachments: Option[Attachments])

case class Load(watermark: Long, walLength: Int)
case class Save(snapshot: Snapshot)
case class WriteAheadLog(mode: ReplayMode.ReplayMode, seq: Long, w: Post)
case class Flush(snapshot: Snapshot)

object Storage {
	val NORM = 1L << 63
	def tombstone(entrant: String) = Post(ReplayMode.TOMBSTONE, entrant, None)
	def tombstone() = Post(ReplayMode.TOMBSTONE, "", None)
}

trait Storage extends Actor {

	val cluster: Cassandra
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

class Reader(override val cluster: Cassandra, val id: String, val leaderboard: LeaderboardBuffer) extends Storage {
	import QueryBuilder._
	case class PageReadRequest(page: Int)

	val readPageStatement = session.prepare(
		select("scoredata")
			.from("pages")
			.where(QueryBuilder.eq("id", bindMarker()))
			.and(QueryBuilder.eq("watermark", bindMarker()))
			.and(QueryBuilder.eq("page", bindMarker()))).setConsistencyLevel(ConsistencyLevel.ONE)

	def loadRows(page: Int): Future[Seq[Row[Long, String]]] = {
		val select = readPageStatement.bind(id)
		session.executeAsync(select) map { resultSet =>
			resultSet.all.zipWithIndex.map { case (r, i) => parseRow(i, r) }
		}
	}

	val readWal = session.prepare(
		select("seq", "scoredata")
			.from("wal")
			.where(QueryBuilder.eq("id", bindMarker()))
			.and(QueryBuilder.eq("watermark", bindMarker())))
		.setConsistencyLevel(ConsistencyLevel.ONE)

	def decodeAttachments(s: String) = s match {
		case "" => None
		case o => Some(Attachments(o))
	}

	def decodeScore(s: String) = s match {
		case "" => ReplayMode.TOMBSTONE
		case o => o.toLong
	}

	def replayWal(sender: ActorRef) = {
		var watermark = 0
		var walLength = 0
		val select = readWal.bind(id, JLong.valueOf(watermark))
		session.executeAsync(select) map { results =>
			leaderboard.replay(
				results.map { r =>
					// "${mode};${w.score};${timestamp};${w.entrant};${encode(w.attachments)}"
					val timestamp = r.getLong("seq")
					val s = r.getString("scoredata").split(';')
					walLength += 1
					Replay(ReplayMode(s(0).toInt), decodeScore(s(1)), timestamp, s(2), if (s.size < 4) None else decodeAttachments(s(3)))
				})
		} onComplete {
			_ => sender ! Load(watermark, walLength)
		}
	}

	def parseRow(i: Int, r: CassandraRow): Row[Long, String] = {
		val score = r.getLong("score")
		val entrant = r.getString("entrant")
		Row(score, entrant, i)
	}

	def receive = {
		case Load => replayWal(sender)
		case PageReadRequest(page) => loadRows(page) pipeTo sender
	}
}

object Writer {
	def props(cluster: Cassandra, id: String, watermark: Long) = Props(new Writer(cluster, id, watermark))
}

class Writer(override val cluster: Cassandra, val id: String, initialWatermark: Long) extends Storage {
	var watermark = initialWatermark;
	lazy val storeWalStatement = session.prepare(QueryBuilder.insertInto("wal")
		.value("id", QueryBuilder.bindMarker())
		.value("watermark", QueryBuilder.bindMarker())
		.value("seq", QueryBuilder.bindMarker())
		.value("scoredata", QueryBuilder.bindMarker()))
		.setConsistencyLevel(ConsistencyLevel.ONE)

	def encode(attachments: Option[Attachments]) = attachments map { s => new String(s.data.toArray, "UTF8") } getOrElse ("")
	def encode(score: Long) = if (score == ReplayMode.TOMBSTONE) "" else score.toString

	def storeWal(mode: ReplayMode.ReplayMode, timestamp: Long, w: Post) = {
		val seq = JLong.valueOf(timestamp)
		val wm = JLong.valueOf(watermark)
		val scoredata = s"${mode.id};${encode(w.score)};${w.entrant};${encode(w.attachments)}"
		val statement = storeWalStatement.bind(id, wm, seq, scoredata)
		session.executeAsync(statement) map { _ => w }
	}

	lazy val storePageStatement = session.prepare(QueryBuilder.insertInto("pages")
		.value("id", QueryBuilder.bindMarker())
		.value("watermark", QueryBuilder.bindMarker())
		.value("page", QueryBuilder.bindMarker())
		.value("scoredata", QueryBuilder.bindMarker()))
		.setConsistencyLevel(ConsistencyLevel.ONE)

	def storeRows(watermark: Long, page: Int, rows: Seq[Entry]): Future[Int] = {
		val scoredata = rows map { row =>
			// TODO: attachments need printf escaping
			s"${encode(row.score)};${row.timestamp};${row.entrant};${row.rank};${encode(row.attachments)}"
		} mkString ("\n")
		val statement = storePageStatement.bind(id, JLong.valueOf(watermark), Integer.valueOf(page), scoredata)
		session.executeAsync(statement) map { _ => page }
	}

	case class PageWriteRequest(page: Int)

	def receive = {
		case WriteAheadLog(mode, seq, w) =>
			val src = sender()
			storeWal(mode, seq, w) foreach { src ! WriteAheadLog(mode, seq, _) }

		case Save(snapshot) =>
			this.watermark = snapshot.timestamp
			val src = sender()
			Future.sequence(snapshot.entries.grouped(1000).zipWithIndex.map {
				case (pageItems, pageIndex) => storeRows(snapshot.timestamp, pageIndex, pageItems)
			}) onSuccess {
				case _ => src ! Save
			}
	}
}