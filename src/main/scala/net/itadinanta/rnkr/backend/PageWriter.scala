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

case class PageReadRequest(id: String, page: Int)
case class PageWriteRequest(id: String, page: Int, rows: Seq[Row[Long, String]])

trait PageIoActor extends Actor {
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

class PageReaderActor(override val cluster: Cassandra, val id: String) extends PageIoActor {

	val query = QueryBuilder.select("score", "entrant").from("pages").where(QueryBuilder.eq("id", "?"))
	val readPageStatement = session.prepare(query).setConsistencyLevel(ConsistencyLevel.ONE)

	def loadRows(page: Int): Future[Seq[Row[Long, String]]] = {
		val select = readPageStatement.bind(id)
		session.executeAsync(select) map { resultSet =>
			resultSet.all.zipWithIndex.map { case (r, i) => parseRow(i, r) }
		}
	}

	def parseRow(i: Int, r: CassandraRow): Row[Long, String] = {
		val score = r.getLong("score")
		val entrant = r.getString("entrant")
		Row(score, entrant, i)
	}

	def receive = {
		case PageReadRequest(id, page) => loadRows(page) pipeTo sender()
	}
}

class PageWriterActor(override val cluster: Cassandra, val id: String) extends PageIoActor {

	val query = QueryBuilder.insertInto("pages").value("id", "?").value("score", "?").value("entrant", "?")
	val insertRowStatement = session.prepare(query).setConsistencyLevel(ConsistencyLevel.ONE)

	def storeRows(id: String, page: Int, rows: Seq[Row[Long, String]]): Future[Boolean] = {
		val batch = new BatchStatement(BatchStatement.Type.UNLOGGED)
		rows foreach { row => batch.add(insertRowStatement.bind(id + "/" + page, java.lang.Long.valueOf(row.key), row.value)) }
		session.executeAsync(batch) map { _ => true }
	}

	def receive = {
		case PageWriteRequest(id, page, rows) => storeRows(id, page, rows) pipeTo sender()
	}
}