package net.itadinanta.rnkr.tree

import net.itadinanta.rnkr.node._
import scala.annotation.tailrec

trait NodeFactory[K, V, ChildType, NodeType <: AbstractNode[K, V, ChildType]] {
	val ordering: Ordering[K]
	def newNode(keys: Seq[K], children: Seq[ChildType]): NodeType
	def newNode(k: K, v: ChildType): NodeType = newNode(Seq(k), Seq(v))
	def newNode(): NodeType = newNode(Seq(), Seq())

	case class BuildResult(val a: NodeType, val b: NodeType, val key: K, val child: ChildType, val index: Int) {
		def node = a
		def this(a: NodeType, key: K, child: ChildType, index: Int) = this(a, a, key, child, index)
	}

	def insert(node: NodeType, k: K, child: ChildType): BuildResult = insert(node.keys, node.values, k, child)
	def delete(node: NodeType, k: K): BuildResult = delete(node.keys, node.values, k)
	def update(node: NodeType, k: K, child: ChildType): BuildResult = update(node.keys, node.values, k, child)
	def split(node: NodeType, position: Int): BuildResult = split(node.keys, node.values, position)
	def join(a: NodeType, b: NodeType): BuildResult = join(a.keys, a.values, b.keys, b.values)

	private def join(aKeys: Seq[K], aValues: Seq[ChildType], bKeys: Seq[K], bValues: Seq[ChildType]) =
		new BuildResult(newNode(aKeys ++ bKeys, aValues ++ bValues), bKeys.head, bValues.head, aKeys.size)

	private def split(keys: Seq[K], children: Seq[ChildType], position: Int) =
		BuildResult(newNode(keys.slice(0, position), children.slice(0, position)),
			newNode(keys.slice(position + 1, keys.size), children.slice(position + 1, keys.size)),
			keys(position), children(position), position)

	private def insert(keys: Seq[K], children: Seq[ChildType], k: K, child: ChildType) = {
		if (keys.isEmpty)
			new BuildResult(newNode(Seq(k), Seq(child)), k, child, 0)
		else {
			val position = keys.indexWhere(ordering.gt(k, _)) + 1
			new BuildResult(newNode(
				keys.take(position) ++ Seq(k) ++ keys.slice(position, keys.size),
				children.take(position) ++ Seq(child) ++ children.slice(position, children.size)),
				k, child, position)
		}
	}

	private def delete(keys: Seq[K], children: Seq[ChildType], key: K) = {
		val position = keys.indexOf(key)
		if (position < 0) throw new IndexOutOfBoundsException("key " + key + " does not exist")

		new BuildResult(newNode(
			keys.take(position) ++ keys.slice(position + 1, children.size),
			children.take(position) ++ children.slice(position + 1, children.size)),
			key, children(position), position)
	}

	private def update(keys: Seq[K], children: Seq[ChildType], k: K, child: ChildType) = {
		val position = keys.indexOf(k)
		if (position < 0) throw new IndexOutOfBoundsException("key " + k + " does not exist")

		new BuildResult(newNode(
			keys,
			children.take(position) ++ Seq(child) ++ children.slice(position + 1, children.size)),
			k, children(position), position)
	}
}

class SeqNodeFactory[K, V](val ordering: Ordering[K] = IntAscending) {


	object Index extends NodeFactory[K, V, Node[K, V], AbstractNode[K, V, Node[K, V]]] {
		class SeqNodeImpl[ChildType](val keys: Seq[K], val values: Seq[ChildType], val parent: IndexNode[K, V] = null)
			extends AbstractNode[K, V, ChildType]
		override val ordering = SeqNodeFactory.this.ordering
		override def newNode(keys: Seq[K], children: Seq[Node[K, V]]) = new SeqNodeImpl(keys, children)
	}

	object Data extends NodeFactory[K, V, V, LeafNode[K, V]] {
		class SeqNodeImpl(val keys: Seq[K],
						  val values: Seq[V],
						  val parent: IndexNode[K, V] = null,
						  val prev: LeafNode[K, V] = null,
						  val next: LeafNode[K, V] = null) extends LeafNode[K, V]
		override val ordering = SeqNodeFactory.this.ordering
		override def newNode(keys: Seq[K], children: Seq[V]) = new SeqNodeImpl(keys, children)
	}

}

trait Tree[K, V] {

	case class Cursor(k: K, node: LeafNode[K, V], index: Int)

	val ordering: Ordering[K]
	val factory = new SeqNodeFactory[K, V](ordering)
	val b: Int

	var root: AbstractNode[K, V, _] = factory.Data.newNode()
	var head: LeafNode[K, V]
	var tail: LeafNode[K, V]


	def put(k: K, value: V): Cursor = {
		val targetNode = search(k)
		val index = targetNode.indexOfKey(k)
		if (index >= 0) {
			val updatedNode = factory.Data.update(targetNode, k, value);
			Cursor(k, updatedNode.node, index)
		}
		else {
			val updatedNode = factory.Data.update(targetNode, k, value);
			Cursor(k, updatedNode.node, index)
		}
	}

	def search(k: K): LeafNode[K, V] = search(k, root).asInstanceOf[LeafNode[K, V]];

	private def search(k: K, n: AbstractNode[K, V, _]): AbstractNode[K, V, _] = {
		n match {
			case l: LeafNode[K, V] => l
			case c: IndexNode[K, V] => search(k, c.childAt(indexBound(0, k, c.keys)))
		}
	}

	@tailrec private def indexBound(n: Int, key: K, keys: Seq[K]): Int =
		if (keys.isEmpty || ordering.le(key, keys.head)) n else indexBound(n + 1, key, keys.tail)
}