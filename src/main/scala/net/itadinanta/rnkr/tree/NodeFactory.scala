package net.itadinanta.rnkr.tree

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import org.slf4j.LoggerFactory
import scala.collection.mutable.ArraySeq
import Rank.Position

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
	private val SPLIT_OFFSET: Int = 1

	def newNode(keys: Seq[K], children: Seq[ChildType], counts: Seq[Position]): NodeType

	def updateNode(input: NodeType, keys: Seq[K], children: Seq[ChildType], counts: Seq[Position]): NodeType
	def updatePartialRanks(node: NodeType, counts: Seq[Position]): NodeType

	def append(input: NodeType, k: K, child: ChildType, partialRank: Position): BuildResult = {
		val keys = input.keys
		val children = input.values
		val partialRanks = input.partialRanks
		if (keys.isEmpty)
			new BuildResult(updateNode(input, Seq(k), Seq(child), Seq(partialRank)), k, child, 0)
		else {
			val newKeys = keys ++ Seq(k)
			val newChildren = children ++ Seq(child)
			val newPartialRanks = partialRanks ++ Seq(partialRank)

			if (newKeys.length <= fanout)
				new BuildResult(updateNode(input, newKeys, newChildren, newPartialRanks), k, child, children.size)
			else
				split(input, minout, newKeys, newChildren, newPartialRanks)
		}
	}

	def insert(input: NodeType, k: K, child: ChildType, take: Position, give: Position): BuildResult = {
		val keys = input.keys
		val children = input.values
		val partialRanks = input.partialRanks
		if (keys.isEmpty)
			new BuildResult(updateNode(input, Seq(k), Seq(child), Seq(take + give)), k, child, 0)
		else {
			val position = keys.lastIndexWhere(item => ordering.gt(k, item)) + 1
			val (keyHead, keyTail) = keys.splitAt(position)
			val (childHead, childTail) = children.splitAt(position + SPLIT_OFFSET)
			val (countHead, countTail) = partialRanks.splitAt(position + SPLIT_OFFSET)
			val newKeys = keyHead ++ Seq(k) ++ keyTail
			val newChildren = childHead ++ Seq(child) ++ childTail
			val first = take + countHead.lastOption.getOrElse(0L)
			val newCounts = countHead.dropRight(1) ++ Seq(first, first + give) ++ countTail.map(_ + take + give)

			if (newKeys.length <= fanout)
				new BuildResult(updateNode(input, newKeys, newChildren, newCounts), k, child, position + SPLIT_OFFSET)
			else
				split(input, minout, newKeys, newChildren, newCounts)
		}
	}

	def delete(node: NodeType, k: K): BuildResult = deleteAt(node, node.indexOfKey(k))
	def deleteAt(input: NodeType, position: Int): BuildResult = {
		val keys = input.keys
		val children = input.values
		val counts = input.partialRanks
		val dropped = counts(position + SPLIT_OFFSET) - counts.lift(position + SPLIT_OFFSET - 1).getOrElse(0L) 
		new BuildResult(updateNode(input,
			keys.take(position) ++ keys.drop(position + 1),
			children.take(position + SPLIT_OFFSET) ++ children.drop(position + SPLIT_OFFSET + 1),
			counts.take(position + SPLIT_OFFSET) ++ counts.drop(position + SPLIT_OFFSET + 1).map(_ - dropped)),
			keys(position), children(position + SPLIT_OFFSET), position)
	}

	def updateAt(input: NodeType, position: Int, child: ChildType, count: Position) = {
		val keys = input.keys
		val children = input.values
		val counts = input.partialRanks

		new BuildResult(updateNode(input,
			keys,
			children.take(position) ++ Seq(child) ++ children.drop(position + 1),
			counts.take(position) ++ Seq(count) ++ counts.drop(position + 1)),
			keys(position), children(position), position)
	}

	def grow(node: NodeType, position: Int, increment: Position): NodeType = {
		if (increment == 0)
			node
		else {
			val counts = node.partialRanks
			updatePartialRanks(node, counts.take(position) ++ counts.drop(position).map(_ + increment))
		}
	}

	def offload(node: NodeType, position: Int, aDelta: Position, bDelta: Position = 0): NodeType = {
		if (aDelta == 0 && bDelta == 0)
			node
		else if (bDelta == 0) {
			val counts = node.partialRanks
			updatePartialRanks(node, counts.take(position) ++
				Seq(counts(position) + aDelta) ++
				counts.drop(position + 1))
		} else {
			val counts = node.partialRanks
			updatePartialRanks(node, counts.take(position) ++
				Seq(counts(position) + aDelta, counts(position + 1) + bDelta) ++
				counts.drop(position + 2))
		}
	}

	private def merge(input: NodeType, aKeys: Seq[K], aValues: Seq[ChildType], aCounts: Seq[Position], bKeys: Seq[K], bValues: Seq[ChildType], bCounts: Seq[Position]) = {
		val newKeys = aKeys ++ bKeys
		val newChildren = aValues ++ bValues
		val newCounts = aCounts ++ bCounts

		if (newKeys.length <= fanout)
			new BuildResult(updateNode(input, newKeys, newChildren, newCounts), bKeys.head, bValues.head, aKeys.size)
		else
			split(input, minout, newKeys, newChildren, newCounts)
	}

	private def split(input: NodeType, splitAt: Int, keys: Seq[K], children: Seq[ChildType], counts: Seq[Position]): BuildResult = {
		val (newKeyHead, newKeyTail) = keys.splitAt(splitAt)
		val splitPoint = splitAt + SPLIT_OFFSET
		val (newChildHead, newChildTail) = children.splitAt(splitPoint)
		val (newPartialRankHead, newPartialRankTail) = counts.splitAt(splitPoint)

		val baseCount = newPartialRankHead.last
		val a = updateNode(input, newKeyHead, newChildHead, newPartialRankHead)
		val b = newNode(newKeyTail.drop(SPLIT_OFFSET), newChildTail, newPartialRankTail.map(_ - baseCount))
		BuildResult(a, b, keys(splitAt), children(splitAt), splitPoint)
	}

	def update(node: NodeType, k: K, child: ChildType, count: Position): BuildResult = updateAt(node, node.indexOfKey(k), child, count)

	def merge(a: NodeType, b: NodeType): BuildResult = merge(a, a.keys, a.values, a.partialRanks, b.keys, b.values, b.partialRanks)

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
			BuildResult(
				updateNode(a, a.keys ++ pivot ++ b.keys,
					a.values ++ b.values,
					a.partialRanks ++ b.partialRanks.map(_ + a.partialRanks.last)),
				updateNode(b, Seq(), Seq(), Seq()),
				removedKey, removedValue,
				a.keys.size)
		} else {
			val keys = a.keys ++ pivot ++ b.keys
			val children = a.values ++ b.values
			val counts = a.partialRanks ++ b.partialRanks.map(_ + a.partialRanks.last)
			val splitAt = (children.size + 1) / 2

			val (newKeyHead, newKeyTail) = keys.splitAt(splitAt - 1)
			val (newChildHead, newChildTail) = children.splitAt(splitAt)
			val (newCountHead, newCountTail) = counts.splitAt(splitAt)

			val mergeKey = newKeyTail.head
			BuildResult(updateNode(a, newKeyHead, newChildHead, newCountHead),
				updateNode(b, newKeyTail.tail, newChildTail, newCountTail.map(_ - newCountHead.last)),
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

	def insert(input: NodeType, k: K, child: ChildType, count: Position): BuildResult = {
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

	def delete(node: NodeType, k: K) = deleteAt(node, node.indexOfKey(k))
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

		private[SeqNodeFactory] class SeqNodeImpl(var keys: Seq[K], var values: Seq[Node[K]], var partialRanks: Seq[Position]) extends IndexNode[K] {
			override def isEmpty = this.keys.isEmpty
			def set(keys: Seq[K], values: Seq[Node[K]], counts: Seq[Long]): this.type = {
				this.keys = keys
				this.values = values
				this.partialRanks = counts
				this
			}
		}

		override val fanout = SeqNodeFactory.this.fanout
		override val ordering = SeqNodeFactory.this.ordering
		override def newNode(keys: Seq[K], children: Seq[Node[K]], partialRanks: Seq[Position]) = new SeqNodeImpl(keys, children, partialRanks)
		override def updateNode(node: IndexNode[K], keys: Seq[K], children: Seq[Node[K]], partialRanks: Seq[Position]) =
			node match { case n: SeqNodeImpl => n.set(keys, children, partialRanks) }

		override def updatePartialRanks(node: IndexNode[K], partialRanks: Seq[Position]) =
			node match { case n: SeqNodeImpl => n.set(n.keys, n.values, partialRanks) }

		override def updateKeys(node: IndexNode[K], keys: Seq[K]) =
			node match { case n: SeqNodeImpl => n.set(keys, n.values, n.partialRanks) }
	}

	override val data = new DataNodeBuilder[K, V] {

		private[SeqNodeFactory] class SeqNodeImpl(k: Seq[K], v: Seq[V]) extends LeafNode[K, V] {
			var _keys: ArraySeq[K] = k.to[ArraySeq]
			var _values: ArraySeq[V] = v.to[ArraySeq]
			def keys = _keys
			def values = _values
			override var prev: LeafNode[K, V] = _
			override var next: LeafNode[K, V] = _
			override def isEmpty = this.keys.isEmpty

			def set(keys: Seq[K], values: Seq[V]): this.type = {
				this._keys = keys.to[ArraySeq]
				this._values = values.to[ArraySeq]
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
