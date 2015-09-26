package net.itadinanta.rnkr.backend.blackhole

import akka.actor.Props
import net.itadinanta.rnkr.backend.Metadata
import net.itadinanta.rnkr.backend.Storage.Reader
import net.itadinanta.rnkr.backend.Storage.Writer
import scala.concurrent.Future
import net.itadinanta.rnkr.backend.Watermark
import net.itadinanta.rnkr.engine.Leaderboard
import net.itadinanta.rnkr.backend.ReplayMode

object BlackHole {
	class BlackHoleReader(
			val datastore: Datastore,
			val id: String) extends Reader {
		implicit override lazy val executionContext = context.dispatcher

		def append(l: List[Future[Page]], pageCount: Int): Future[Int] =
			Future.successful(0)

		def loadPage(watermark: Long, page: Int): Future[Page] =
			Future.successful(Page(0, Seq()))

		def loadMetadata(): Future[Metadata] =
			Future.successful(Metadata())

		def loadWatermark(): Future[Watermark] =
			Future.successful(Watermark(0, 0))

		def replayWal(watermark: Long): Future[Int] =
			Future.successful(0)
	}

	class BlackHoleWriter(
			val datastore: Datastore,
			val id: String,
			override val initialWatermark: Long,
			override val metadata: Metadata) extends Writer {
		implicit override lazy val executionContext = context.dispatcher

		def compact(watermark: Watermark): Unit =
			Unit

		def storeRows(page: Int, rows: Seq[Leaderboard.Entry]): Future[Int] =
			Future.successful(rows.size)

		def storeWal(mode: ReplayMode.Value, timestamp: Long, watermark: Long, w: Leaderboard.Post): Future[Leaderboard.Post] =
			Future.successful(w)

		def storeWatermark(watermark: Long, pages: Int): Future[Watermark] =
			Future.successful(Watermark(watermark, pages))
	}

	class Datastore extends net.itadinanta.rnkr.backend.Datastore {
		override def readerProps(id: String) =
			Props(new BlackHoleReader(this, id))
		override def writerProps(id: String, watermark: Long, metadata: Metadata) =
			Props(new BlackHoleWriter(this, id, watermark, metadata))
	}
}