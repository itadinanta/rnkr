package net.itadinanta.rnkr.arbiter

import akka.actor.{ ActorRef, Actor, FSM }
import scala.concurrent.Future
import akka.actor.ActorSystem
import akka.actor.Props
import akka.util.Timeout
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration._
import akka.pattern.{ ask, pipe }
import net.itadinanta.rnkr.tree.Row
import net.itadinanta.rnkr.tree.RankedTreeMap
import net.itadinanta.rnkr.tree.Rank.Position
import akka.actor.PoisonPill
import scala.reflect.ClassTag
import akka.actor.ActorContext

object Arbiter {
	def create[T](t: T)(implicit context: ActorContext) = new ActorArbiter(t)
}

trait Arbiter[T] {
	val target: T
	def wqueue[R](f: T => R)(implicit t: ClassTag[R]): Future[R]
	def rqueue[R](f: T => R)(implicit t: ClassTag[R]): Future[R]
	def shutdown()
}

trait TreeArbiter[K, V] extends Arbiter[RankedTreeMap[K, V]] {
	def put(k: K, v: V) = wqueue(_.put(k, v))
	def append(k: K, v: V) = wqueue(_.append(k, v))
	def remove(k: K) = wqueue(_.remove(k))

	def size = rqueue(_.size)
	def version = rqueue(_.version)
	def get(k: K) = rqueue(_.get(k))
	def keys() = rqueue(_.keys())
	def keysReverse() = rqueue(_.keysReverse())
	def rank(k: K) = rqueue(_.rank(k))
	def range(k: K, length: Int) = rqueue(_.range(k, length))
	def page(start: Position, length: Int) = rqueue(_.page(start, length))
}

object TreeArbiter {
	def create[K, V](t: RankedTreeMap[K, V])(implicit context: ActorContext) = new ActorArbiter(t) with TreeArbiter[K, V]
}

class ActorArbiter[T](val target: T)(implicit val context: ActorContext) extends Arbiter[T] {
	implicit lazy val executionContext = context.system.dispatcher
	implicit val timeout = Timeout.intToTimeout(Int.MaxValue)
	val gate = context.actorOf(Props(new Gate(target)))

	sealed trait Response
	case object ReadResponse extends Response
	case object WriteResponse extends Response

	sealed trait Request[R] { val f: T => R; val replyTo: Option[ActorRef] }
	case class ReadRequest[R](f: T => R, replyTo: Option[ActorRef] = None) extends Request[R]
	case class WriteRequest[R](f: T => R, replyTo: Option[ActorRef] = None) extends Request[R]

	override def wqueue[R](f: T => R)(implicit t: ClassTag[R]): Future[R] = ask(gate, WriteRequest(f)).mapTo[R]
	override def rqueue[R](f: T => R)(implicit t: ClassTag[R]): Future[R] = ask(gate, ReadRequest(f)).mapTo[R]
	override def shutdown() { gate ! PoisonPill }

	class Gate(val target: T) extends Actor {
		import scala.collection.immutable.Queue
		case class State(val rc: Int, val wc: Int, val q: Queue[Request[_]])
		var state = State(0, 0, Queue[Request[_]]())

		@tailrec private def flush(s: State): State = if (s.q.isEmpty) s else
			s.q.dequeue match {
				case (ReadRequest(f, Some(replyTo)), tail) if (s.wc == 0) => {
					Future { replyTo ! f(target); ReadResponse } pipeTo self
					flush(State(s.rc + 1, s.wc, tail))
				}
				case (WriteRequest(f, Some(replyTo)), tail) if (s.wc == 0 && s.rc == 0) => {
					Future { replyTo ! f(target); WriteResponse } pipeTo self
					State(s.rc, s.wc + 1, tail)
				}
				case _ => s
			}

		def next(s: State) { state = flush(s) }
		def receive() = {
			case ReadRequest(f, None) => next(state.copy(q = state.q.enqueue(ReadRequest(f, Some(sender)))))
			case WriteRequest(f, None) => next(state.copy(q = state.q.enqueue(WriteRequest(f, Some(sender)))))
			case ReadResponse => next(state.copy(rc = state.rc - 1))
			case WriteResponse => next(state.copy(wc = state.wc - 1))
		}
	}
}

