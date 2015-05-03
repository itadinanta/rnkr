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
import akka.pattern.pipe
import net.itadinanta.rnkr.engine.leaderboard.Update
import net.itadinanta.rnkr.backend.ReplayMode
import net.itadinanta.rnkr.backend.Storage
import net.itadinanta.rnkr.backend.Flush
import net.itadinanta.rnkr.backend.Save

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
			case Load(watermark, walLength) =>
				import UpdateMode._
				reader ! PoisonPill
				writer = context.actorOf(Writer.props(cassandra, name, watermark), "writer_" + name)

				val leaderboard = new LeaderboardDecorator(LeaderboardArbiter.wrap(context.actorOf(Gate.props(target), "gate_" + name))) {
					var flushCount: Int = walLength
					implicit val timeout = Timeout(1 minute)
					def onUpdate(update: Update) = {
						flushCount += 1
						if (flushCount > 10) {
							super.export() onSuccess { case snapshot => self ! Flush(snapshot) }
							flushCount = 0
						}
						update
					}

					override def post(post: Post, updateMode: UpdateMode) = super.post(post, updateMode) flatMap { update =>
						writer ask WriteAheadLog(ReplayMode(updateMode), update.timestamp, post) map { _ => onUpdate(update) }
					}

					override def remove(entrant: String) = super.remove(entrant) flatMap { update =>
						writer ask WriteAheadLog(ReplayMode.Delete, update.timestamp, Storage.tombstone(entrant)) map { _ => onUpdate(update) }
					}

					override def clear() = super.clear() flatMap { update =>
						writer ask WriteAheadLog(ReplayMode.Clear, update.timestamp, Storage.tombstone()) map { _ => onUpdate(update) }
					}
				}

				arbiter.success(leaderboard)

			case Flush(snapshot) =>
				writer ! Save(snapshot)

		}
	}

	val lifecycle = actorRefFactory.actorOf(Props(new LifecycleActor), "lifecycle_" + name)
}