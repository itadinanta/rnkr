package net.itadinanta.rnkr.tree

import net.itadinanta.rnkr.node._
import scala.annotation.tailrec


trait Tree[K, V] {
	case class Cursor(k: K, node: Node[K, V], index: Int)

	val ordering: Ordering[K]
	val b: Int

	var root: AbstractNode[K, V, _]
	var head: LeafNode[K, V]
	var tail: LeafNode[K, V]

	def search(k: K): LeafNode[K, V] = search(k, root).asInstanceOf[LeafNode[K,V]];

	private def search(k: K, n: AbstractNode[K, V, _]): AbstractNode[K, V, _] = {
		n match {
			case l: LeafNode[K, V] => l
			case c: IndexNode[K, V] => search(k, c.childAt(indexBound(0, k, c.keys)))
		}
	}

	@tailrec private def indexBound(n: Int, key: K, keys: Seq[K]): Int =
		if (keys.isEmpty || ordering.le(key, keys.head)) n else indexBound(n + 1, key, keys.tail)
}