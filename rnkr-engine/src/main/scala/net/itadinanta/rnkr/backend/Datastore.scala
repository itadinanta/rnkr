package net.itadinanta.rnkr.backend

import akka.actor.Props
import net.itadinanta.rnkr.engine.LeaderboardBuffer

trait Datastore {
	def readerProps(id: String): Props
	def writerProps(id: String, watermark: Long, metadata: Metadata): Props
}