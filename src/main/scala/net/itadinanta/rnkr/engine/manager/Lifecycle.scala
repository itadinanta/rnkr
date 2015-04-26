package net.itadinanta.rnkr.engine.manager

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.DurationConversions._
import scala.concurrent.duration.FiniteDuration
import akka.actor.Actor
import akka.actor.ActorContext
import akka.actor.ActorRefFactory
import akka.actor.Props
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout
import net.itadinanta.rnkr.backend.Cassandra
import net.itadinanta.rnkr.engine.leaderboard.LeaderboardBuffer
import net.itadinanta.rnkr.engine.leaderboard.LeaderboardArbiter
import scala.concurrent.Promise
import net.itadinanta.rnkr.backend.PageReaderActor
import net.itadinanta.rnkr.backend.PageWriterActor
import net.itadinanta.rnkr.core.arbiter.Gate
import net.itadinanta.rnkr.core.tree.Row
import net.itadinanta.rnkr.engine.leaderboard.Leaderboard
import net.itadinanta.rnkr.backend.PageReadRequest
import akka.actor.PoisonPill

class Lifecycle(name: String, cassandra: Cassandra, constructor: () => LeaderboardBuffer, actorRefFactory: ActorRefFactory) {
	implicit val executionContext = actorRefFactory.dispatcher
	val arbiter = Promise[Leaderboard]
	def leaderboard: Future[Leaderboard] = arbiter.future

	class LifecycleActor extends Actor {
		val target = constructor()
		val writer = context.actorOf(PageWriterActor.props(cassandra, name), "writer_" + name)

		val reader = context.actorOf(PageReaderActor.props(cassandra, name), "reader_" + name)
		reader ! PageReadRequest(name, 0)

		def receive: Actor.Receive = {
			case page: Seq[Row[Long, String]] => {
				//target.init()
				reader ! PoisonPill
				val gate = context.actorOf(Gate.props(target), "gate_" + name)
				arbiter.success(LeaderboardArbiter.wrap(gate))
			}
		}
	}

	val lifecycle = actorRefFactory.actorOf(Props(new LifecycleActor), "lifecycle_" + name)
}