package net.itadinanta.rnkr.core.arbiter

import akka.actor.{ ActorRef, Actor, FSM }
import scala.concurrent.Future
import akka.actor.ActorSystem
import akka.actor.Props
import akka.util.Timeout
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration._
import akka.pattern.{ ask, pipe }
import akka.actor.PoisonPill
import scala.reflect.ClassTag
import akka.actor.ActorContext
import akka.actor.ActorRefFactory
import grizzled.slf4j.Logging

object Arbiter {
	def create[T](t: T, context: ActorRefFactory) = new ActorArbiter(t, context)
}

trait Arbiter[T] {
	def wqueue[R](f: T => R)(implicit t: ClassTag[R]): Future[R]
	def rqueue[R](f: T => R)(implicit t: ClassTag[R]): Future[R]
	def shutdown()
}

object Gate {
	def props[T](target: T) = Props(new Gate(target))

	sealed trait Response
	case object ReadResponse extends Response
	case object WriteResponse extends Response

	sealed trait Request[T, R] { val f: T => R; val replyTo: Option[ActorRef] }
	case class ReadRequest[T, R](f: T => R, replyTo: Option[ActorRef] = None) extends Request[T, R]
	case class WriteRequest[T, R](f: T => R, replyTo: Option[ActorRef] = None) extends Request[T, R]
}

class Gate[T](val target: T) extends Actor with Logging {
	implicit lazy val executionContext = context.system.dispatcher
	import Gate._

	import scala.collection.immutable.Queue
	case class State(val rc: Int, val wc: Int, val q: Queue[Request[T, _]])
	var state = State(0, 0, Queue[Request[T, _]]())

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
		case r: ReadRequest[T, _] if r.replyTo == None => next(state.copy(q = state.q.enqueue(ReadRequest(r.f, Some(sender)))))
		case w: WriteRequest[T, _] if w.replyTo == None => next(state.copy(q = state.q.enqueue(WriteRequest(w.f, Some(sender)))))
		case ReadResponse => next(state.copy(rc = state.rc - 1))
		case WriteResponse => next(state.copy(wc = state.wc - 1))
	}
}

trait GateWrapper[T] extends Arbiter[T] {
	val gate: ActorRef
	implicit val timeout = Timeout(1 day)
	import Gate._

	override def wqueue[R](f: T => R)(implicit t: ClassTag[R]): Future[R] = ask(gate, WriteRequest(f)).mapTo[R]
	override def rqueue[R](f: T => R)(implicit t: ClassTag[R]): Future[R] = ask(gate, ReadRequest(f)).mapTo[R]
	override def shutdown() { gate ! PoisonPill }
}

class ActorGateWrapper[T](override val gate: ActorRef) extends GateWrapper[T]

class ActorArbiter[T](val target: T, factory: ActorRefFactory) extends ActorGateWrapper[T](factory.actorOf(Gate.props(target)))

