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
import net.itadinanta.rnkr.engine.leaderboard.Replay

case object Load
case object Save
case class WriteAheadLog(w: Post, mode: UpdateMode, seq: Long)

object Storage {
	val NORM = 1L << 63
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
		select("score", "entrant")
			.from("pages")
			.where(QueryBuilder.eq("id", bindMarker()))).setConsistencyLevel(ConsistencyLevel.ONE)

	def loadRows(page: Int): Future[Seq[Row[Long, String]]] = {
		val select = readPageStatement.bind(id)
		session.executeAsync(select) map { resultSet =>
			resultSet.all.zipWithIndex.map { case (r, i) => parseRow(i, r) }
		}
	}

	val readWal = session.prepare(
		select("seq", "scoredata")
			.from("wal")
			.where(QueryBuilder.eq("id", bindMarker()))).setConsistencyLevel(ConsistencyLevel.ONE)

	def decode(s: String) = s match {
		case "" => None
		case o => Some(Attachments(BaseEncoding.base64().decode(o)))
	}

	def replayWal(sender: ActorRef) = {
		val select = readWal.bind(id)
		session.executeAsync(select) map { results =>
			leaderboard.replay(
				results.map { r =>
					val timestamp = r.getLong("seq")
					// "${mode};${w.score};${timestamp};${w.entrant};${encode(w.attachments)}"
					val scoredata = r.getBytes("scoredata")
					val scoredataBuffer = new Array[Byte](scoredata.remaining())
					scoredata.get(scoredataBuffer)
					val scoredataString = new String(scoredataBuffer, "UTF8")
					val s = scoredataString.split(';')

					val mode = UpdateMode.withName(s(0))
					val score = s(1).toLong

					val entrant = s(3)
					val attachments = if (s.size < 5) None else decode(s(4))
					Replay(mode, score, timestamp, entrant, attachments)
				})
		} onComplete {
			_ => sender ! Load
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
	def props(cluster: Cassandra, id: String, leaderboard: LeaderboardArbiter) = Props(new Writer(cluster, id, leaderboard))
}

class Writer(override val cluster: Cassandra, val id: String, val leaderboard: LeaderboardArbiter) extends Storage {
	import java.lang.{ Long => JLong }
	lazy val storeWalStatement = session.prepare(QueryBuilder.insertInto("wal")
		.value("id", QueryBuilder.bindMarker())
		.value("seq", QueryBuilder.bindMarker())
		.value("scoredata", QueryBuilder.bindMarker()))
		.setConsistencyLevel(ConsistencyLevel.ONE)

	def encode(attachments: Option[Attachments]) = attachments map { s => BaseEncoding.base64().encode(s.data.toArray) } getOrElse ("")

	def storeWal(w: Post, mode: UpdateMode, timestamp: Long) = {
		val seq = JLong.valueOf(timestamp)
		val scoredata = s"${mode};${w.score};${timestamp};${w.entrant};${encode(w.attachments)}"
		val statement = storeWalStatement.bind(id, seq, ByteBuffer.wrap(scoredata.getBytes))
		session.executeAsync(statement) map { _ => w }
	}

	lazy val storePageRowStatement = session.prepare(QueryBuilder.insertInto("pages")
		.value("id", QueryBuilder.bindMarker())
		.value("score", QueryBuilder.bindMarker())
		.value("entrant", QueryBuilder.bindMarker()))
		.setConsistencyLevel(ConsistencyLevel.ONE)

	def storeRows(page: Int, rows: Seq[Entry]): Future[Int] = {
		val batch = new BatchStatement(BatchStatement.Type.UNLOGGED)
		rows foreach { row =>
			val score = (BigInt(1) << 128) | (BigInt(row.score + Storage.NORM)) << 64 | BigInt(row.timestamp)
			val entrant = s"${row.entrant};${encode(row.attachments)}"
			batch.add(storePageRowStatement.bind(id + "/" + page, ByteBuffer.wrap(score.toByteArray), ByteBuffer.wrap(entrant.getBytes)))
		}
		session.executeAsync(batch) map { _ => page }
	}

	case class PageWriteRequest(page: Int)

	def receive = {
		case WriteAheadLog(w, mode, seq) => {
			storeWal(w, mode, seq) map { WriteAheadLog(_, mode, seq) } pipeTo sender
		}

		case Save => {
			leaderboard.page(0, Int.MaxValue) flatMap { allRows =>
				Future.sequence(allRows.grouped(1000).zipWithIndex.map {
					case (pageItems, pageIndex) => storeRows(pageIndex, pageItems)
				})
			}
		}
	}
}