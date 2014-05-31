package net.itadinanta.rnkr.tree

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

import org.slf4j.LoggerFactory

trait NodeBuilder[K, V, ChildType, NodeType <: Node[K] with Children[ChildType]] {
	val LOG = LoggerFactory.getLogger(this.getClass())
	val ordering: Ordering[K]
	val fanout: Int
	def minout = (fanout + 1) / 2
	val splitOffset: Int
	def updateNode(input: NodeType, keys: Seq[K], children: Seq[ChildType], counts: Seq[Rank#Position]): NodeType
	def updateKeys(node: NodeType, keys: Seq[K]): NodeType
	def updateCounts(node: NodeType, counts: Seq[Rank#Position]): NodeType
	def newNode(keys: Seq[K], children: Seq[ChildType], counts: Seq[Rank#Position]): NodeType
	def newNode(k: K, v: ChildType): NodeType = newNode(Seq(k), Seq(v), Seq(1))
	def newNode(): NodeType = newNode(Seq(), Seq(), Seq())

	case class BuildResult(val a: NodeType, val b: NodeType, val key: K, val child: ChildType, val index: Int) {
		def node = a
		def split = !(a eq b)
		def this(a: NodeType, key: K, child: ChildType, index: Int) = this(a, a, key, child, index)
	}

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

	def delete(node: NodeType, k: K): BuildResult = deleteAt(node, node.indexOfKey(k))

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

	def update(node: NodeType, k: K, child: ChildType, count: Rank#Position): BuildResult = updateAt(node, node.indexOfKey(k), child, count)

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

	def renameKey(node: NodeType, oldKey: K, newKey: K): NodeType = if (oldKey != newKey) renameKeyAt(node, node.indexOfKey(oldKey), newKey) else node
	def renameKeyAt(input: NodeType, position: Int, newKey: K): NodeType = {
		val keys = input.keys
		if (position < 0 || position >= keys.size || keys(position) == newKey) input
		else updateKeys(input, keys.take(position) ++ Seq(newKey) ++ keys.drop(position + 1))
	}

	def grow(node: NodeType, position: Int, increment: Rank#Position): NodeType = {
		if (increment == 0) node
		else {
			val counts = node.counts
			updateCounts(node, counts.take(position) ++ Seq(counts(position) + increment) ++ counts.drop(position + 1))
		}
	}

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
			if (keys.size == children.size) {
				// merge and split evenly
				val (newKeyHead, newKeyTail) = keys.splitAt(splitAt)
				val (newChildHead, newChildTail) = children.splitAt(splitAt)
				val (newCountHead, newCountTail) = counts.splitAt(splitAt)

				val mergeKey = b.keys.head
				BuildResult(updateNode(a, newKeyHead, newChildHead, newCountHead),
					updateNode(b, newKeyTail, newChildTail, newCountTail),
					mergeKey, removedValue,
					splitAt)
			} else {
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

	def balanced(n: Node[K]) = n.keys.size >= minout
}

abstract class NodeFactory[K, V](val ordering: Ordering[K], val fanout: Int = 10) {
	type IndexNodeBuilder = NodeBuilder[K, V, Node[K], IndexNode[K]]
	type DataNodeBuilder = NodeBuilder[K, V, V, LeafNode[K, V]]
	val index: IndexNodeBuilder
	val data: DataNodeBuilder
}

class SeqNodeFactory[K, V](ordering: Ordering[K] = IntAscending, fanout: Int = 10) extends NodeFactory[K, V](ordering, fanout) {
	override val index = new IndexNodeBuilder {

		private[SeqNodeFactory] class SeqNodeImpl(var keys: Seq[K], var values: Seq[Node[K]], var counts: Seq[Rank#Position]) extends IndexNode[K] with Children[Node[K]] {
			override def isEmpty = this.keys.isEmpty
			def set(keys: Seq[K], values: Seq[Node[K]], counts: Seq[Long]): this.type = {
				this.keys = keys
				this.values = values
				this.counts = counts
				this
			}
		}

		override val splitOffset = 1
		override val fanout = SeqNodeFactory.this.fanout
		override val ordering = SeqNodeFactory.this.ordering
		override def newNode(keys: Seq[K], children: Seq[Node[K]], counts: Seq[Rank#Position]) = new SeqNodeImpl(keys, children, counts)
		override def updateNode(node: IndexNode[K], keys: Seq[K], children: Seq[Node[K]], counts: Seq[Rank#Position]) = node match {
			case n: SeqNodeImpl => n.set(keys, children, counts)
		}

		override def updateCounts(node: IndexNode[K], counts: Seq[Rank#Position]) = node match {
			case n: SeqNodeImpl => n.set(n.keys, n.values, counts)
		}

		override def updateKeys(node: IndexNode[K], keys: Seq[K]) = node match {
			case n: SeqNodeImpl => n.set(keys, n.values, n.counts)
		}
	}

	override val data = new DataNodeBuilder {

		private[SeqNodeFactory] class SeqNodeImpl(var keys: Seq[K], var values: Seq[V]) extends LeafNode[K, V] {
			override var prev: LeafNode[K, V] = _
			override var next: LeafNode[K, V] = _
			override def counts = Seq.fill(this.keys.size)(1: Long)
			override def isEmpty = this.keys.isEmpty

			def set(keys: Seq[K], values: Seq[V]): this.type = {
				this.keys = keys
				this.values = values
				this
			}
		}

		override val splitOffset = 0
		override val fanout = SeqNodeFactory.this.fanout
		override val ordering = SeqNodeFactory.this.ordering
		override def newNode(keys: Seq[K], children: Seq[V], counts: Seq[Rank#Position]) = new SeqNodeImpl(keys, children)
		override def updateNode(node: LeafNode[K, V], keys: Seq[K], children: Seq[V], counts: Seq[Rank#Position]) = node match {
			case n: SeqNodeImpl => n.set(keys, children)
		}
		override def updateKeys(node: LeafNode[K, V], keys: Seq[K]) = node match {
			case n: SeqNodeImpl => n.set(keys, n.values)
		}
		override def updateCounts(node: LeafNode[K, V], counts: Seq[Rank#Position]) = node

	}
}

trait BPlusTree[K, V] {
	def size: Int
	def get(k: K): Option[Row[K, V]]
	def remove(k: K): Option[Row[K, V]]
	def put(k: K, value: V): Row[K, V]
	def append(k: K, value: V): Row[K, V]
	def keys(): Seq[K]
	def keysReverse(): Seq[K]
	def rank(k: K): Rank#Position
	def range(k: K, length: Int): Seq[Row[K, V]]
}

class SeqBPlusTree[K, V](val factory: NodeFactory[K, V]) extends BPlusTree[K, V] {
	val LOG = LoggerFactory.getLogger(this.getClass())
	case class Cursor(val key: K, val value: V, val node: LeafNode[K, V], val index: Int)

	private[rnkr] var head: LeafNode[K, V] = factory.data.newNode()
	private[rnkr] var root: Node[K] = head
	private[rnkr] var tail: LeafNode[K, V] = head
	private[rnkr] var _size: Int = 0
	private[rnkr] var leafCount: Int = 1
	private[rnkr] var indexCount: Int = 0
	private[rnkr] var level: Int = 1

	override def size = _size
	override def rank(k: K): Rank#Position = ???
	override def append(k: K, v: V) = {
		val inserted = insertAtEnd(k, v)
		Row(k, inserted.value, size)
	}
	override def put(k: K, v: V) = {
		val inserted = insert(k, v)
		Row(k, v, inserted.index)
	}

	override def get(k: K) = {
		val node = search(k)
		val index = node.indexOfKey(k)
		if (index >= 0) {
			val value = node.childAt(index)
			Some(Row(k, value, index))
		} else None
	}

	override def keys() = {
		val buf = new ListBuffer[K]
		def appendNode(node: LeafNode[K, V]): Unit = if (node != null) {
			buf ++= node.keys
			appendNode(node.next)
		}
		appendNode(head)
		buf.toList
	}

	override def keysReverse() = {
		val buf = new ListBuffer[K]
		def appendNode(node: LeafNode[K, V]): Unit = if (node != null) {
			buf ++= node.keys.reverse
			appendNode(node.prev)
		}
		appendNode(tail)
		buf.toList
	}

	override def remove(k: K) = delete(k) match {
		case Cursor(key, child, _, index) => Some(Row(key, child, index))
		case _ => None
	}

	override def toString(): String = {
		val buf = new StringBuilder()
		buf.append("{").append("size=").append(_size).append("/").append(leafCount).append("/").append(indexCount)
		buf.append(root)
		buf.append("}").toString()
	}

	def min(node: Node[K]): K = node match {
		case data: LeafNode[K, V] => data.keys.reduceLeft { factory.data.ordering.min }
		case index: IndexNode[K] => index.values map (min(_)) reduceLeft { factory.data.ordering.min }
	}

	def first(node: Node[K]): K = node match {
		case data: LeafNode[K, V] => data.keys.head
		case index: IndexNode[K] => first(index.values.head)
	}

	def consistent(node: Node[K]): Boolean = node match {
		case data: LeafNode[K, V] =>
			(data.keys.size == data.values.size) &&
				((data.next eq null) && (tail eq data)) || ((data.next ne null) && (data.next.prev eq data)) &&
				((data.prev eq null) && (head eq data)) || ((data.prev ne null) && (data.prev.next eq data))

		case index: IndexNode[K] =>
			(index.keys.size == (index.values.size - 1)) &&
				(index.isEmpty ||
					(factory.index.ordering.lt(min(index.values.head), index.keys.head) &&
						index.keys.zip(index.values.tail.map(min)).foldLeft(true) { (a, b) => a && (b._1 == b._2) } &&
						index.values.foldLeft(true) { (a, b) => (a && consistent(b)) }))
	}

	def consistent: Boolean = consistent(root)

	private def seek(k: K): Cursor = {
		val leaf = search(k)
		val index = leaf.indexOfKey(k)
		Cursor(k, leaf.childAt(index), leaf, index)
	}

	private def insertAtEnd(k: K, v: V): Cursor = {
		case class AppendedResult(a: Node[K], key: K, b: Option[Node[K]], cursor: Cursor)

		def appendRecursively(targetNode: Node[K], k: K, v: V): AppendedResult = targetNode match {
			case leaf: LeafNode[K, V] => {
				if (leaf.isEmpty || factory.ordering.gt(k, leaf.keys.last)) {
					val appended = factory.data.append(leaf, k, v, 1)
					_size += 1
					val cursor = Cursor(k, v, leaf, appended.index)
					if (appended.split) {
						appended.b.next = appended.a.next
						appended.a.next = appended.b
						tail = appended.b
						tail.prev = appended.a
						leafCount += 1
						AppendedResult(appended.a, appended.key, Some(appended.b), cursor)
					} else
						AppendedResult(appended.a, appended.key, None, cursor)
				} else throw new IllegalArgumentException("Key " + k + " out of order")
			}
			case index: IndexNode[K] => {
				if (factory.ordering.gt(k, index.keys.last)) {
					appendRecursively(index.values.last, k, v) match {
						case AppendedResult(a, newKey, None, cursor) => AppendedResult(index, newKey, None, cursor)
						case AppendedResult(a, newKey, Some(b), cursor) => {
							val inserted = factory.index.append(index, newKey, b, 1)
							indexCount += (if (inserted.split) 1 else 0)
							AppendedResult(inserted.a, inserted.key, if (inserted.split) Some(inserted.b) else None, cursor)
						}
					}
				} else throw new IllegalArgumentException("Key " + k + " out of order")
			}
		}

		val appended = appendRecursively(root, k, v)
		root = appended match {
			case AppendedResult(a, key, Some(b), cursor) => newRoot(key, a, b)
			case _ => root
		}

		appended.cursor
	}

	private def insert(k: K, v: V): Cursor = {
		case class InsertedResult(a: Node[K], aGrow: Int, key: K, b: Option[Node[K]], bGrow: Int, cursor: Cursor)

		def insertRecursively(targetNode: Node[K], k: K, v: V): InsertedResult = targetNode match {
			case leaf: LeafNode[K, V] => {
				val i = targetNode.indexOfKey(k)
				if (i >= 0)
					InsertedResult(leaf, 0, k, None, 0, Cursor(k, v, leaf, factory.data.update(leaf, k, v, 1).index))
				else {
					val inserted = factory.data.insert(leaf, k, v, 1)
					_size += 1
					val cursor = Cursor(k, v, leaf, inserted.index)
					if (inserted.split) {
						inserted.b.next = leaf.next
						inserted.a.next = inserted.b
						inserted.b.prev = inserted.a
						if (inserted.b.next != null) inserted.b.next.prev = inserted.b
						head = if (leaf eq head) inserted.a else head
						tail = if (leaf eq tail) inserted.b else tail
						leafCount += 1
						val aGrow = if (inserted.index < inserted.a.keys.size) +1 else 0
						InsertedResult(inserted.a, aGrow, inserted.key, Some(inserted.b), 1 - aGrow, cursor)
					} else
						InsertedResult(inserted.a, +1, inserted.key, None, 0, cursor)
				}
			}
			case index: IndexNode[K] => {
				val i = index.keys.lastIndexWhere(factory.ordering.ge(k, _)) + 1
				insertRecursively(index.childAt(i), k, v) match {
					case InsertedResult(a, aGrow, newKey, None, bGrow, cursor) => {
						InsertedResult(factory.index.grow(index, i, aGrow), aGrow, newKey, None, bGrow, cursor)
					}
					case InsertedResult(a, aGrow, newKey, Some(b), bGrow, cursor) => {
						val inserted = factory.index.insert(index, newKey, b, 1)
						if (inserted.split) {
							indexCount += 1
							InsertedResult(inserted.a, aGrow, inserted.key, Some(inserted.b), bGrow, cursor)
						} else InsertedResult(index, aGrow, inserted.key, None, bGrow, cursor)
					}
				}
			}
		}

		val inserted = insertRecursively(root, k, v)
		root = inserted match {
			case InsertedResult(a, aGrow, key, Some(b), bGrow, cursor) => newRoot(key, a, b)
			case _ => root
		}

		inserted.cursor
	}

	private def newRoot(key: K, a: Node[K], b: Node[K]) = {
		level += 1
		indexCount += 1
		factory.index.newNode(Seq(key), Seq(a, b), Seq(1, 1))
	}

	private def delete(k: K): Cursor = {
		case class DeletedResult(pos: Int,
			ak: K,
			oldAk: K,
			a: Node[K],
			bk: Option[K],
			oldBk: Option[K],
			cursor: Cursor)

		def deleteRecursively(k: K, pos: Int, left: Option[Node[K]], targetNode: Node[K], right: Option[Node[K]]): DeletedResult = {
			// LOG.debug("Removing {} from {}", k, targetNode)
			// choose siblings
			lazy val (i, av, bv) = (left, right) match {
				case (None, Some(rv)) => (pos, targetNode, rv)
				case (Some(lv), None) => (pos - 1, lv, targetNode)
				case (Some(lv), Some(rv)) => if (lv.keys.size > rv.keys.size) (pos - 1, lv, targetNode) else (pos, targetNode, rv)
				case (None, None) => (pos, targetNode, targetNode)
			}

			targetNode match {
				case leaf: LeafNode[K, V] => {
					val leafK = leaf.keys.head
					val deleted = factory.data.delete(leaf, k)
					_size -= 1
					val cursor = Cursor(k, deleted.child, leaf, deleted.index)
					if (factory.data.balanced(leaf) || (av eq bv))
						DeletedResult(i, if (leaf.isEmpty) k else leaf.keyAt(0), leafK, leaf, None, None, cursor)
					else (av, bv) match {
						case (a: LeafNode[K, V], b: LeafNode[K, V]) => {
							val oldAk = if (a eq leaf) leafK else a.keys.head
							val oldBk = if (b eq leaf) leafK else b.keys.head
							val balanced = factory.data.redistribute(a, Seq(), b)

							if (balanced.b.isEmpty) {
								// special case for LeafNode
								balanced.a.next = balanced.b.next
								if (balanced.a.next != null) balanced.a.next.prev = balanced.a
								if (balanced.b eq tail) tail = balanced.a
								leafCount -= 1
							}
							DeletedResult(i, balanced.a.keyAt(0), oldAk, balanced.a, balanced.b.keyOption(0), Some(oldBk), cursor)
						}
					}
				}
				case index: IndexNode[K] => {
					val sub = index.keys.lastIndexWhere(factory.ordering.ge(k, _)) + 1
					deleteRecursively(k, sub, index.childOption(sub - 1), index.childAt(sub), index.childOption(sub + 1)) match {
						case DeletedResult(u, ak, oldAk, a, None, Some(oldPivot), cursor) => {
							if (sub > 0 && index.keyAt(sub - 1) == oldAk) factory.index.renameKeyAt(index, sub - 1, ak)
							val deleted = factory.index.deleteAt(index, u)
							if (factory.data.balanced(index) || (av eq bv)) {
								DeletedResult(pos, ak, oldAk, index, None, None, cursor)
							} else (av, bv) match {
								case (a: IndexNode[K], b: IndexNode[K]) => {
									val pivot = first(b)
									val oldBk = if (b eq index) oldPivot else b.keys.head
									val balanced = factory.index.redistribute(a, Seq(pivot), b)
									if (balanced.b.isEmpty) {
										indexCount -= 1
										DeletedResult(i, ak, oldAk, balanced.a, None, Some(pivot), cursor)
									} else {
										DeletedResult(i, ak, oldAk, balanced.a, Some(balanced.key), Some(pivot), cursor)
									}
								}
							}
						}

						case DeletedResult(u, ak, oldAk, a, Some(bk), oldBk, cursor) => {
							if (ak != oldAk && sub > 0 && index.keyAt(sub - 1) == oldAk) factory.index.renameKeyAt(index, sub - 1, ak)
							oldBk foreach { k =>
								if (bk != k) {
									if (sub > 0 && index.keyAt(sub - 1) == k) factory.index.renameKeyAt(index, sub - 1, bk)
									else if (index.keyAt(sub) == k) factory.index.renameKeyAt(index, sub, bk)
								}
							}
							DeletedResult(sub - 1, ak, oldAk, index, None, None, cursor)
						}

						case DeletedResult(u, k, oldK, node, None, None, cursor) => {
							if (k != oldK && sub > 0 && index.keyAt(sub - 1) == oldK) factory.index.renameKeyAt(index, sub - 1, k)
							DeletedResult(sub - 1, k, oldK, index, None, None, cursor)
						}
					}
				}
			}
		}

		deleteRecursively(k, 0, None, root, None) match {
			case DeletedResult(_, _, _, _, _, _, cursor) => {
				root = root match {
					case index: IndexNode[K] => if (index.isEmpty) {
						indexCount -= 1;
						level -= 1;
						index.childAt(0)
					} else root
					case _ => root
				}
				cursor
			}
		}
	}

	override def range(k: K, length: Int): Seq[Row[K, V]] = {

		@tailrec def rangeForwards(buf: ListBuffer[Row[K, V]], position: Rank#Position, khead: Seq[K], vhead: Seq[V], next: LeafNode[K, V], leftover: Int) {
			if (leftover > 0) {
				if (khead == Nil) {
					if (next != null) rangeForwards(buf, position, next.keys, next.values, next.next, leftover)
				} else {
					buf.append(Row(khead.head, vhead.head, position))
					rangeForwards(buf, position + 1, khead.tail, vhead.tail, next, leftover - 1)
				}
			}
		}

		@tailrec def rangeBackwards(buf: ListBuffer[Row[K, V]], position: Rank#Position, khead: Seq[K], vhead: Seq[V], prev: LeafNode[K, V], leftover: Int) {
			if (leftover > 0) {
				if (khead == Nil) {
					if (prev != null) rangeBackwards(buf, position, prev.keys.reverse, prev.values.reverse, prev.prev, leftover)
				} else {
					buf.append(Row(khead.head, vhead.head, position))
					rangeBackwards(buf, position - 1, khead.tail, vhead.tail, prev, leftover - 1)
				}
			}
		}

		val leaf = search(k)
		val buf = new ListBuffer[Row[K, V]]
		if (length >= 0) {
			val index = after(k, leaf.keys)
			rangeForwards(buf, index, leaf.keys.drop(index), leaf.values.drop(index), leaf.next, length)
		} else {
			val index = before(k, leaf.keys)
			rangeBackwards(buf, index, leaf.keys.take(index).reverse, leaf.values.take(index).reverse, leaf.prev, -length)
		}
		buf.toSeq
	}

	private def after(key: K, keys: Seq[K]): Int = keys.lastIndexWhere(factory.ordering.gt(key, _)) + 1
	private def before(key: K, keys: Seq[K]): Int = keys.lastIndexWhere(factory.ordering.ge(key, _)) + 1
	private def search(k: K): LeafNode[K, V] = pathTo(k, root, Nil, after).head.asInstanceOf[LeafNode[K, V]]
	private def pathTo(k: K): List[Node[K]] = pathTo(k, root, Nil, before)
	private def pathTo(k: K, n: Node[K], path: List[Node[K]], indexBound: (K, Seq[K]) => Int): List[Node[K]] = {
		n match {
			case l: LeafNode[K, V] => l :: path
			case c: IndexNode[K] => pathTo(k, c.childAt(indexBound(k, c.keys)), c :: path, indexBound)
		}
	}
}