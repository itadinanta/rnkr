package net.itadinanta.rnkr.tree

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

import org.slf4j.LoggerFactory

trait NodeBuilder[K, V, ChildType, NodeType <: Node[K] with Children[ChildType]] {
	val LOG = LoggerFactory.getLogger(this.getClass())
	val ordering: Ordering[K]
	val fanout: Int
	def minout = (fanout + 1) / 2

	case class BuildResult(val a: NodeType, val b: NodeType, val key: K, val child: ChildType, val index: Int) {
		def node = a
		def split = !(a eq b)
		def this(a: NodeType, key: K, child: ChildType, index: Int) = this(a, a, key, child, index)
	}

	def delete(node: NodeType, k: K): BuildResult = deleteAt(node, node.indexOfKey(k))
	def deleteAt(input: NodeType, position: Int): BuildResult

	def updateKeys(node: NodeType, keys: Seq[K]): NodeType
	def renameKey(node: NodeType, oldKey: K, newKey: K): NodeType =
		if (oldKey != newKey) renameKeyAt(node, node.indexOfKey(oldKey), newKey) else node
	def renameKeyAt(input: NodeType, position: Int, newKey: K): NodeType = {
		val keys = input.keys
		if (position < 0 || position >= keys.size || keys(position) == newKey) input
		else updateKeys(input, keys.take(position) ++ Seq(newKey) ++ keys.drop(position + 1))
	}

	def balanced(n: Node[K]) = n.keys.size >= minout
}

abstract class IndexNodeBuilder[K, V] extends NodeBuilder[K, V, Node[K], IndexNode[K]] {
	type NodeType = IndexNode[K]
	type ChildType = Node[K]
	private val splitOffset: Int = 1

	def newNode(keys: Seq[K], children: Seq[ChildType], counts: Seq[Rank#Position]): NodeType

	def updateNode(input: NodeType, keys: Seq[K], children: Seq[ChildType], counts: Seq[Rank#Position]): NodeType
	def updateCounts(node: NodeType, counts: Seq[Rank#Position]): NodeType

	def append(input: NodeType, k: K, child: ChildType, count: Rank#Position): BuildResult = {
		val keys = input.keys
		val children = input.values
		val counts = input.counts
		if (keys.isEmpty)
			new BuildResult(updateNode(input, Seq(k), Seq(child), Seq(count)), k, child, 0)
		else {
			val newKeys = keys ++ Seq(k)
			val newChildren = children ++ Seq(child)
			val newCounts = counts ++ Seq(count)

			if (newKeys.length <= fanout)
				new BuildResult(updateNode(input, newKeys, newChildren, newCounts), k, child, children.size)
			else
				split(input, minout, newKeys, newChildren, newCounts)
		}
	}

	def insert(input: NodeType, k: K, child: ChildType, count: Rank#Position): BuildResult = {
		val keys = input.keys
		val children = input.values
		val counts = input.counts
		if (keys.isEmpty)
			new BuildResult(updateNode(input, Seq(k), Seq(child), Seq(count)), k, child, 0)
		else {
			val position = keys.lastIndexWhere(item => ordering.gt(k, item)) + 1
			val (keyHead, keyTail) = keys.splitAt(position)
			val (childHead, childTail) = children.splitAt(position + splitOffset)
			val (countHead, countTail) = counts.splitAt(position + splitOffset)
			val newKeys = keyHead ++ Seq(k) ++ keyTail
			val newChildren = childHead ++ Seq(child) ++ childTail
			val newCounts = countHead ++ Seq(count) ++ countTail

			if (newKeys.length <= fanout)
				new BuildResult(updateNode(input, newKeys, newChildren, newCounts), k, child, position + splitOffset)
			else
				split(input, minout, newKeys, newChildren, newCounts)
		}
	}

	def deleteAt(input: NodeType, position: Int): BuildResult = {
		val keys = input.keys
		val children = input.values
		val counts = input.counts
		new BuildResult(updateNode(input,
			keys.take(position) ++ keys.drop(position + 1),
			children.take(position + splitOffset) ++ children.drop(position + splitOffset + 1),
			counts.take(position + splitOffset) ++ counts.drop(position + splitOffset + 1)),
			keys(position), children(position + splitOffset), position)
	}

	def updateAt(input: NodeType, position: Int, child: ChildType, count: Rank#Position) = {
		val keys = input.keys
		val children = input.values
		val counts = input.counts

		new BuildResult(updateNode(input,
			keys,
			children.take(position) ++ Seq(child) ++ children.drop(position + 1),
			counts.take(position) ++ Seq(count) ++ counts.drop(position + 1)),
			keys(position), children(position), position)
	}

	def grow(node: NodeType, position: Int, increment: Rank#Position): NodeType = {
		if (increment == 0) node
		else {
			val counts = node.counts
			updateCounts(node, counts.take(position) ++ Seq(counts(position) + increment) ++ counts.drop(position + 1))
		}
	}

	private def merge(input: NodeType, aKeys: Seq[K], aValues: Seq[ChildType], aCounts: Seq[Rank#Position], bKeys: Seq[K], bValues: Seq[ChildType], bCounts: Seq[Rank#Position]) = {
		val newKeys = aKeys ++ bKeys
		val newChildren = aValues ++ bValues
		val newCounts = aCounts ++ bCounts

		if (newKeys.length <= fanout)
			new BuildResult(updateNode(input, newKeys, newChildren, newCounts), bKeys.head, bValues.head, aKeys.size)
		else
			split(input, minout, newKeys, newChildren, newCounts)
	}

	private def split(input: NodeType, splitAt: Int, keys: Seq[K], children: Seq[ChildType], counts: Seq[Rank#Position]): BuildResult = {
		val (newKeyHead, newKeyTail) = keys.splitAt(splitAt)
		val (newChildHead, newChildTail) = children.splitAt(splitAt + splitOffset)
		val (newCountHead, newCountTail) = counts.splitAt(splitAt + splitOffset)

		BuildResult(updateNode(input, newKeyHead, newChildHead, newCountHead),
			newNode(newKeyTail.drop(splitOffset), newChildTail, newCountTail),
			keys(splitAt), children(splitAt),
			splitAt + splitOffset)
	}

	def update(node: NodeType, k: K, child: ChildType, count: Rank#Position): BuildResult = updateAt(node, node.indexOfKey(k), child, count)

	def merge(a: NodeType, b: NodeType): BuildResult = merge(a, a.keys, a.values, a.counts, b.keys, b.values, b.counts)

	def redistribute(a: NodeType, pivot: Seq[K], b: NodeType): BuildResult = {
		val aSize = a.keys.size
		val bSize = b.keys.size
		val removedKey = b.keys.head
		val removedValue = b.values.head
		if (balanced(a) && balanced(b))
			// do nothing
			BuildResult(a, b, removedKey, removedValue, a.keys.size)
		else if (aSize + bSize < fanout) {
			// migrate everything to the first node
			BuildResult(updateNode(a, a.keys ++ pivot ++ b.keys, a.values ++ b.values, a.counts ++ b.counts),
				updateNode(b, Seq(), Seq(), Seq()),
				removedKey, removedValue,
				a.keys.size)
		} else {
			val keys = a.keys ++ pivot ++ b.keys
			val children = a.values ++ b.values
			val counts = a.counts ++ b.counts
			val splitAt = (children.size + 1) / 2

			val (newKeyHead, newKeyTail) = keys.splitAt(splitAt - 1)
			val (newChildHead, newChildTail) = children.splitAt(splitAt)
			val (newCountHead, newCountTail) = counts.splitAt(splitAt)

			val mergeKey = newKeyTail.head
			BuildResult(updateNode(a, newKeyHead, newChildHead, newCountHead),
				updateNode(b, newKeyTail.tail, newChildTail, newCountTail),
				mergeKey, removedValue,
				splitAt)
		}
	}

}

abstract class DataNodeBuilder[K, V] extends NodeBuilder[K, V, V, LeafNode[K, V]] {
	type NodeType = LeafNode[K, V]
	type ChildType = V

	def newNode(keys: Seq[K], children: Seq[ChildType]): NodeType
	def newNode(k: K, v: ChildType): NodeType = newNode(Seq(k), Seq(v))
	def newNode(): NodeType = newNode(Seq(), Seq())

	def updateNode(input: NodeType, keys: Seq[K], children: Seq[ChildType]): NodeType

	def insert(input: NodeType, k: K, child: ChildType, count: Rank#Position): BuildResult = {
		val keys = input.keys
		val children = input.values

		if (keys.isEmpty)
			new BuildResult(updateNode(input, Seq(k), Seq(child)), k, child, 0)
		else {
			val position = keys.lastIndexWhere(item => ordering.gt(k, item)) + 1
			val (keyHead, keyTail) = keys.splitAt(position)
			val (childHead, childTail) = children.splitAt(position)
			val newKeys = keyHead ++ Seq(k) ++ keyTail
			val newChildren = childHead ++ Seq(child) ++ childTail

			if (newKeys.length <= fanout)
				new BuildResult(updateNode(input, newKeys, newChildren), k, child, position)
			else
				split(input, minout, newKeys, newChildren)
		}
	}

	def update(node: NodeType, k: K, child: ChildType): BuildResult = updateAt(node, node.indexOfKey(k), child)
	def updateAt(input: NodeType, position: Int, child: ChildType) = {
		val keys = input.keys
		val children = input.values

		new BuildResult(updateNode(input,
			keys,
			children.take(position) ++ Seq(child) ++ children.drop(position + 1)),
			keys(position), children(position), position)
	}

	def append(input: NodeType, k: K, child: ChildType): BuildResult = {
		val keys = input.keys
		val children = input.values

		if (keys.isEmpty)
			new BuildResult(updateNode(input, Seq(k), Seq(child)), k, child, 0)
		else {
			val newKeys = keys ++ Seq(k)
			val newChildren = children ++ Seq(child)

			if (newKeys.length <= fanout)
				new BuildResult(updateNode(input, newKeys, newChildren), k, child, children.size)
			else
				split(input, minout, newKeys, newChildren)
		}
	}

	def deleteAt(input: NodeType, position: Int): BuildResult = {
		val keys = input.keys
		val children = input.values

		new BuildResult(updateNode(input,
			keys.take(position) ++ keys.drop(position + 1),
			children.take(position) ++ children.drop(position + 1)),
			keys(position), children(position), position)
	}

	def redistribute(a: NodeType, pivot: Seq[K], b: NodeType): BuildResult = {
		val aSize = a.keys.size
		val bSize = b.keys.size
		val removedKey = b.keys.head
		val removedValue = b.values.head
		if (balanced(a) && balanced(b))
			// do nothing
			BuildResult(a, b, removedKey, removedValue, a.keys.size)
		else if (aSize + bSize < fanout) {
			// migrate everything to the first node
			BuildResult(updateNode(a, a.keys ++ pivot ++ b.keys, a.values ++ b.values),
				updateNode(b, Seq(), Seq()),
				removedKey, removedValue,
				a.keys.size)
		} else {
			val keys = a.keys ++ pivot ++ b.keys
			val children = a.values ++ b.values
			val splitAt = (children.size + 1) / 2
			// merge and split evenly
			val (newKeyHead, newKeyTail) = keys.splitAt(splitAt)
			val (newChildHead, newChildTail) = children.splitAt(splitAt)
			val mergeKey = b.keys.head

			BuildResult(updateNode(a, newKeyHead, newChildHead),
				updateNode(b, newKeyTail, newChildTail),
				mergeKey, removedValue,
				splitAt)
		}
	}

	private def split(input: NodeType, splitAt: Int, keys: Seq[K], children: Seq[ChildType]): BuildResult = {
		val (newKeyHead, newKeyTail) = keys.splitAt(splitAt)
		val (newChildHead, newChildTail) = children.splitAt(splitAt)

		BuildResult(updateNode(input, newKeyHead, newChildHead),
			newNode(newKeyTail, newChildTail),
			keys(splitAt), children(splitAt),
			splitAt)
	}

}

abstract class NodeFactory[K, V](val ordering: Ordering[K], val fanout: Int = 10) {
	val index: IndexNodeBuilder[K, V]
	val data: DataNodeBuilder[K, V]
}

class SeqNodeFactory[K, V](ordering: Ordering[K] = IntAscending, fanout: Int = 10) extends NodeFactory[K, V](ordering, fanout) {
	override val index = new IndexNodeBuilder[K, V] {

		private[SeqNodeFactory] class SeqNodeImpl(var keys: Seq[K], var values: Seq[Node[K]], var counts: Seq[Rank#Position]) extends IndexNode[K] {
			override def isEmpty = this.keys.isEmpty
			def set(keys: Seq[K], values: Seq[Node[K]], counts: Seq[Long]): this.type = {
				this.keys = keys
				this.values = values
				this.counts = counts
				this
			}
		}

		override val fanout = SeqNodeFactory.this.fanout
		override val ordering = SeqNodeFactory.this.ordering
		override def newNode(keys: Seq[K], children: Seq[Node[K]], counts: Seq[Rank#Position]) = new SeqNodeImpl(keys, children, counts)
		override def updateNode(node: IndexNode[K], keys: Seq[K], children: Seq[Node[K]], counts: Seq[Rank#Position]) =
			node match { case n: SeqNodeImpl => n.set(keys, children, counts) }

		override def updateCounts(node: IndexNode[K], counts: Seq[Rank#Position]) =
			node match { case n: SeqNodeImpl => n.set(n.keys, n.values, counts) }

		override def updateKeys(node: IndexNode[K], keys: Seq[K]) =
			node match { case n: SeqNodeImpl => n.set(keys, n.values, n.counts) }
	}

	override val data = new DataNodeBuilder[K, V] {

		private[SeqNodeFactory] class SeqNodeImpl(var keys: Seq[K], var values: Seq[V]) extends LeafNode[K, V] {
			override var prev: LeafNode[K, V] = _
			override var next: LeafNode[K, V] = _
			override def isEmpty = this.keys.isEmpty

			def set(keys: Seq[K], values: Seq[V]): this.type = {
				this.keys = keys
				this.values = values
				this
			}
		}

		override val fanout = SeqNodeFactory.this.fanout
		override val ordering = SeqNodeFactory.this.ordering
		override def newNode(keys: Seq[K], children: Seq[V]) = new SeqNodeImpl(keys, children)
		override def updateNode(node: LeafNode[K, V], keys: Seq[K], children: Seq[V]) =
			node match { case n: SeqNodeImpl => n.set(keys, children) }

		override def updateKeys(node: LeafNode[K, V], keys: Seq[K]) =
			node match { case n: SeqNodeImpl => n.set(keys, n.values) }
	}
}
