package net.itadinanta.rnkr.core.tree

import akka.actor.ActorContext
import net.itadinanta.rnkr.core.arbiter.Arbiter
import net.itadinanta.rnkr.core.tree.Rank.Position
import net.itadinanta.rnkr.core.arbiter.ActorArbiter

trait TreeArbiter[K, V] extends Arbiter[RankedTreeMap[K, V]] {
	def put(k: K, v: V) = wqueue(_.put(k, v))
	def append(k: K, v: V) = wqueue(_.append(k, v))
	def remove(k: K) = wqueue(_.remove(k))
	def clear() = wqueue(_.clear())
	
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
	def create[K, V](t: RankedTreeMap[K, V])(implicit context: ActorContext) = new ActorArbiter(t, context) with TreeArbiter[K, V]
}

