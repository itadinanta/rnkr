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
import net.itadinanta.rnkr.backend.Reader
import net.itadinanta.rnkr.backend.Writer
import net.itadinanta.rnkr.core.arbiter.Gate
import net.itadinanta.rnkr.core.tree.Row
import net.itadinanta.rnkr.engine.leaderboard.Leaderboard
import net.itadinanta.rnkr.backend.Load
import akka.actor.PoisonPill
import akka.actor.ActorRef
import net.itadinanta.rnkr.backend.Load
import net.itadinanta.rnkr.engine.leaderboard.UpdateMode
import net.itadinanta.rnkr.engine.leaderboard.Post
import net.itadinanta.rnkr.engine.leaderboard.LeaderboardDecorator
import net.itadinanta.rnkr.backend.WriteAheadLog
import scala.concurrent.duration._
import akka.pattern.ask
import net.itadinanta.rnkr.engine.leaderboard.Update

class Lifecycle(name: String, cassandra: Cassandra, constructor: () => LeaderboardBuffer, actorRefFactory: ActorRefFactory) {
	implicit val executionContext = actorRefFactory.dispatcher
	val arbiter = Promise[Leaderboard]
	def leaderboard: Future[Leaderboard] = arbiter.future

	class LifecycleActor extends Actor {

		val target = constructor()
		var writer: ActorRef = _

		val reader = context.actorOf(Reader.props(cassandra, name, target), "reader_" + name)
		reader ! Load

		def receive = {
			case page: Seq[Row[Long, String]] => {
				import UpdateMode._
				reader ! PoisonPill
				val gate = context.actorOf(Gate.props(target), "gate_" + name)
				val l = LeaderboardArbiter.wrap(gate)
				writer = context.actorOf(Writer.props(cassandra, name, l), "writer_" + name)

				arbiter.success(new LeaderboardDecorator(l) {
					override def post(post: Post, updateMode: UpdateMode = BestWins) = {
						implicit val timeout = Timeout(1 minute)
						super.post(post, updateMode) flatMap {
							update => (writer ? WriteAheadLog(post, updateMode, update.newEntry.get.timestamp)) map { _ => update }
						}
					}
				})
			}
		}
	}

	val lifecycle = actorRefFactory.actorOf(Props(new LifecycleActor), "lifecycle_" + name)
}