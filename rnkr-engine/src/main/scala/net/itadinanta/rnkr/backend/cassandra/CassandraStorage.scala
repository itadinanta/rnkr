package net.itadinanta.rnkr.backend.cassandra

import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.BatchStatement
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.ResultSetFuture
import com.datastax.driver.core.Session
import scala.concurrent.Future
import scala.concurrent.Promise
import com.google.common.util.concurrent.FutureCallback
import com.google.common.util.concurrent.Futures
import com.datastax.driver.core.querybuilder.QueryBuilder
import net.itadinanta.rnkr.engine.Leaderboard._
import net.itadinanta.rnkr.engine.Leaderboard.UpdateMode._
import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext
import net.itadinanta.rnkr.backend._
import java.lang.{ Long => JLong }
import scala.language.implicitConversions
import net.itadinanta.rnkr.engine.LeaderboardBuffer

trait CassandraStorage extends Storage {
	val cassandra: Cassandra
	val keyspace: String
	val session = cassandra.cluster.connect(keyspace)
	implicit val executionContext: ExecutionContext

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

object CassandraStorage {
	import Storage._

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

	class CassandraReader(
		override val cassandra: Cassandra,
		override val keyspace: String,
		override val datastore: Datastore,
		override val id: String)
			extends Reader
			with CassandraStorage
			with ReaderStatements {
		implicit override lazy val executionContext = context.dispatcher

		override def loadWatermark() =
			for { wmRow <- session.executeAsync(readLastWatermarkStatement.bind(id)) } yield wmRow.headOption match {
				case Some(data) => Watermark(data.getLong("watermark"), data.getInt("pages"))
				case None => Watermark(0, 0)
			}

		override def loadPage(watermark: Long, page: Int) =
			for {
				resultSet <- session.executeAsync(readPageStatement.bind(id, JLong.valueOf(watermark), Integer.valueOf(page)))
				entries = for {
					row <- resultSet
					line <- row.getString("scoredata").lines
					s = line.split(Storage.CSV_SEPARATOR)
				} yield Entry(decodeScore(s(0)), s(1).toLong, s(2), s(3).toLong, if (s.length < 5) None else decodeAttachments(s(4)))
			} yield Page(page, entries)

		override def append(l: List[Future[Page]], pageCount: Int): Future[Int] =
			if (l.isEmpty)
				Future.successful(pageCount)
			else l.head flatMap { p =>
				leaderboard.append(p.entries)
				append(l.tail, pageCount)
			}

		override def replayWal(watermark: Long) =
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

		override def loadMetadata() =
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

	class CassandraWriter(
		override val cassandra: Cassandra,
		override val keyspace: String,
		override val datastore: Datastore,
		override val id: String,
		override val initialWatermark: Long,
		override val metadata: Metadata)
			extends Writer
			with CassandraStorage
			with WriterStatements {
		override implicit lazy val executionContext = context.dispatcher

		// TODO: attachments need printf escaping
		def storeWal(mode: ReplayMode.Value, timestamp: Long, watermark: Long, w: Post) = {
			val seq = JLong.valueOf(timestamp)
			val wm = JLong.valueOf(watermark)
			val scoredata = Seq(mode.id, encode(w.score), w.entrant, encode(w.attachments)) mkString Storage.CSV_SEPARATOR
			for { _ <- session.executeAsync(storeWalStatement.bind(id, wm, seq, scoredata)) } yield w
		}

		override def storeRows(page: Int, rows: Seq[Entry]): Future[Int] = {
			val scorerows = for { row <- rows } yield Seq(encode(row.score), row.timestamp, row.entrant, row.rank, encode(row.attachments)) mkString Storage.CSV_SEPARATOR
			val scoredata = scorerows mkString "\n"
			val statement = storePageStatement.bind(id, JLong.valueOf(watermark), Integer.valueOf(page), scoredata)
			for { _ <- session.executeAsync(statement) } yield page
		}

		override def storeWatermark(watermark: Long, pages: Int) =
			for { _ <- session.executeAsync(storeWatermarks.bind(id, JLong.valueOf(watermark), Integer.valueOf(pages))) }
				yield Watermark(watermark, pages)

		override def compact(watermark: Watermark) = {
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
	}
}

