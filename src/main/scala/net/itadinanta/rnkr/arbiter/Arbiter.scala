package net.itadinanta.rnkr.arbiter

import akka.actor.ActorRef
import akka.actor.Actor
import scala.concurrent.Future
import akka.actor.ActorSystem
import akka.actor.Props
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration._
import akka.pattern.ask
import net.itadinanta.rnkr.tree.Row
import net.itadinanta.rnkr.tree.BPlusTree
import net.itadinanta.rnkr.tree.Rank.Position
import akka.actor.PoisonPill

object Arbiter {
	def create[K, V](t: BPlusTree[K, V])(implicit system: ActorSystem) = new ActorArbiter(t)
}

trait Arbiter[K, V] {
	type Type = BPlusTree[K, V]

	def wqueue[R](f: (Type) => R): Future[R]
	def rqueue[R](f: (Type) => R): Future[R]

	def put(k: K, v: V): Future[Row[K, V]] = wqueue((target: Type) => target.put(k, v))
	def append(k: K, v: V): Future[Row[K, V]] = wqueue((target: Type) => target.append(k, v))
	def remove(k: K): Future[Option[Row[K, V]]] = wqueue((target: Type) => target.remove(k))

	def get(k: K): Future[Option[Row[K, V]]] = rqueue((target: Type) => target.get(k))
	def keys(): Future[Seq[K]] = rqueue((target: Type) => target.keys())
	def keysReverse(): Future[Seq[K]] = rqueue((target: Type) => target.keysReverse())
	def rank(k: K): Future[Position] = rqueue((target: Type) => target.rank(k))
	def range(k: K, length: Int): Future[Seq[Row[K, V]]] = rqueue((target: Type) => target.range(k, length))
	def page(start: Position, length: Int): Future[Seq[Row[K, V]]] = rqueue((target: Type) => target.page(start, length))
	
	def shutdown()
}

class ActorArbiter[K, V](val target: BPlusTree[K, V])(implicit val system: ActorSystem) extends Arbiter[K, V] {
	implicit lazy val executionContext = system.dispatcher

	sealed trait Request
	case class ReadRequest[R](f: Type => R) extends Request
	case class WriteRequest[R](f: Type => R) extends Request

	implicit val timeout = Timeout(5 seconds)

	val gate = system.actorOf(Props(new Gate(target)))

	def wqueue[R](f: Type => R): Future[R] = gate ? WriteRequest(f) map { _.asInstanceOf[R] }
	def rqueue[R](f: Type => R): Future[R] = gate ? WriteRequest(f) map { _.asInstanceOf[R] }
	def shutdown() { gate ! PoisonPill }

	class Gate[A, R](val target: Type) extends Actor {
		def receive() = {
			case r: WriteRequest[_] => sender ! r.f(target)
			case r: ReadRequest[_] => sender ! r.f(target)
			case _ =>
		}
	}
}

