package net.itadinanta.rnkr.tree

import net.itadinanta.rnkr.node._
import scala.collection.mutable.ListBuffer
import scala.annotation.tailrec

trait NodeBuilder[K, V, ChildType, NodeType <: Node[K] with Children[ChildType]] {
	val ordering: Ordering[K]
	val fanout: Int
	def minout = (fanout + 1) / 2
	val splitOffset: Int
	def updateNode(input: NodeType, keys: Seq[K], children: Seq[ChildType]): NodeType
	def updateKeys(node: NodeType, keys: Seq[K]): NodeType
	def newNode(keys: Seq[K], children: Seq[ChildType]): NodeType
	def newNode(k: K, v: ChildType): NodeType = newNode(Seq(k), Seq(v))
	def newNode(): NodeType = newNode(Seq(), Seq())

	case class BuildResult(val a: NodeType, val b: NodeType, val key: K, val child: ChildType, val index: Int) {
		def node = a
		def split = !(a eq b)
		def this(a: NodeType, key: K, child: ChildType, index: Int) = this(a, a, key, child, index)
	}

	def insert(node: NodeType, k: K, child: ChildType): BuildResult = {
		insert(node, node.keys, node.values, k, child)
	}
	def delete(node: NodeType, k: K): BuildResult = delete(node, node.keys, node.values, k)
	def update(node: NodeType, k: K, child: ChildType): BuildResult = update(node, node.keys, node.values, k, child)
	def merge(a: NodeType, b: NodeType): BuildResult = merge(a, a.keys, a.values, b.keys, b.values)
	def replaceKey(node: NodeType, oldKey: K, newKey: K): NodeType = replaceKey(node, node.keys, oldKey, newKey)

	private def merge(input: NodeType, aKeys: Seq[K], aValues: Seq[ChildType], bKeys: Seq[K], bValues: Seq[ChildType]) = {
		val newKeys = aKeys ++ bKeys
		val newChildren = aValues ++ bValues

		if (newKeys.length <= fanout)
			new BuildResult(updateNode(input, aKeys ++ bKeys, aValues ++ bValues), bKeys.head, bValues.head, aKeys.size)
		else
			split(input, minout, newKeys, newChildren)
	}

	private def insert(input: NodeType, keys: Seq[K], children: Seq[ChildType], k: K, child: ChildType) = {
		if (keys.isEmpty)
			new BuildResult(updateNode(input, Seq(k), Seq(child)), k, child, 0)
		else {
			val position = keys.lastIndexWhere(item => ordering.gt(k, item)) + 1
			val (keyHead, keyTail) = keys.splitAt(position)
			val (childHead, childTail) = children.splitAt(position + splitOffset)
			val newKeys = keyHead ++ Seq(k) ++ keyTail
			val newChildren = childHead ++ Seq(child) ++ childTail

			if (newKeys.length <= fanout)
				new BuildResult(updateNode(input, newKeys, newChildren), k, child, position + splitOffset)
			else
				split(input, minout, newKeys, newChildren)
		}
	}

	private def split(input: NodeType, splitAt: Int, keys: Seq[K], children: Seq[ChildType]): BuildResult = {
		val (newKeyHead, newKeyTail) = keys.splitAt(splitAt)
		val (newChildHead, newChildTail) = children.splitAt(splitAt + splitOffset)

		BuildResult(updateNode(input, newKeyHead, newChildHead),
			newNode(newKeyTail.drop(splitOffset), newChildTail),
			keys(splitAt), children(splitAt),
			splitAt + splitOffset)
	}

	def redistribute(a: NodeType, pivot: Seq[K], b: NodeType): BuildResult = {
		val aSize = a.size
		val bSize = b.size
		val removedKey = b.keys.head
		val removedValue = b.values.head
		if (balanced(a) && balanced(b))
		// do nothing
			BuildResult(a, b, removedKey, removedValue, a.size)
		else if (aSize + bSize <= fanout) {
			// migrate everything to the first node
			BuildResult(updateNode(a, a.keys ++ pivot ++ b.keys, a.values ++ b.values),
				updateNode(b, Seq(), Seq()),
				removedKey, removedValue,
				a.keys.size)
		}
		else {
			// merge and split evenly
			val keys = a.keys ++ pivot ++ b.keys
			val children = a.values ++ b.values
			val splitAt = (aSize + bSize + 1) / 2
			val (newKeyHead, newKeyTail) = keys.splitAt(splitAt)
			val (newChildHead, newChildTail) = children.splitAt(splitAt + splitOffset)

			BuildResult(updateNode(a, newKeyHead, newChildHead),
				updateNode(b, newKeyTail.drop(splitOffset), newChildTail.drop(splitOffset)),
				removedKey, removedValue,
				splitAt + splitOffset)
		}
	}

	private def delete(input: NodeType, keys: Seq[K], children: Seq[ChildType], key: K) = {
		val position = keys.indexOf(key)
		if (position < 0) throw new IndexOutOfBoundsException("key " + key + " does not exist")

		new BuildResult(updateNode(
			input,
			keys.take(position) ++ keys.drop(position + 1),
			children.take(position + splitOffset) ++ children.drop(position + splitOffset + 1)),
			key, children(position + splitOffset), position)
	}

	private def replaceKey(input: NodeType, keys: Seq[K], k: K, newKey: K): NodeType = {
		val position = keys.indexOf(k)
		if (position < 0) throw new IndexOutOfBoundsException("key " + k + " does not exist")

		updateKeys(input, keys.take(position) ++ Seq(newKey))
	}

	private def update(input: NodeType, keys: Seq[K], children: Seq[ChildType], k: K, child: ChildType) = {
		val position = keys.indexOf(k)
		if (position < 0) throw new IndexOutOfBoundsException("key " + k + " does not exist")

		new BuildResult(updateNode(input, keys, children.take(position) ++ Seq(child) ++ children.drop(position + 1)),
			k, children(position), position)
	}

	def balanced(n: Node[K]) = n.size >= minout
}

abstract class NodeFactory[K, V](val ordering: Ordering[K], val fanout: Int = 10) {
	type IndexNodeBuilder = NodeBuilder[K, V, Node[K], IndexNode[K]]
	type DataNodeBuilder = NodeBuilder[K, V, V, LeafNode[K, V]]
	val index: IndexNodeBuilder
	val data: DataNodeBuilder
}

class SeqNodeFactory[K, V](ordering: Ordering[K] = IntAscending, fanout: Int = 10) extends NodeFactory[K, V](ordering, fanout) {
	override val index = new IndexNodeBuilder {

		private[SeqNodeFactory] class SeqNodeImpl(var keys: Seq[K], var values: Seq[Node[K]]) extends IndexNode[K] with Children[Node[K]] {
			override def isEmpty = this.keys.isEmpty
			def set(keys: Seq[K], values: Seq[Node[K]]): this.type = {
				this.keys = keys
				this.values = values
				this
			}
		}

		override val splitOffset = 1
		override val fanout = SeqNodeFactory.this.fanout
		override val ordering = SeqNodeFactory.this.ordering
		override def newNode(keys: Seq[K], children: Seq[Node[K]]) = new SeqNodeImpl(keys, children)
		override def updateNode(node: IndexNode[K], keys: Seq[K], children: Seq[Node[K]]) = node match {
			case n: SeqNodeImpl => n.set(keys, children)
		}
		override def updateKeys(node: IndexNode[K], keys: Seq[K]) = node match {
			case n: SeqNodeImpl => n.set(keys, n.values)
		}
	}

	override val data = new DataNodeBuilder {

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

		override val splitOffset = 0
		override val fanout = SeqNodeFactory.this.fanout
		override val ordering = SeqNodeFactory.this.ordering
		override def newNode(keys: Seq[K], children: Seq[V]) = new SeqNodeImpl(keys, children)
		override def updateNode(node: LeafNode[K, V], keys: Seq[K], children: Seq[V]) = node match {
			case n: SeqNodeImpl => n.set(keys, children)
		}
		override def updateKeys(node: LeafNode[K, V], keys: Seq[K]) = node match {
			case n: SeqNodeImpl => n.set(keys, n.values)
		}
	}
}

trait BPlusTree[K, V] {
	def size: Int
	def get(k: K): Option[V]
	def remove(k: K): Option[V]
	def put(k: K, value: V): V
	def keys(): Seq[K]
	def keysReverse(): Seq[K]
	def range(k: K, length: Int): Seq[Pair[K, V]]
}

class SeqBPlusTree[K, V](val factory: NodeFactory[K, V]) extends BPlusTree[K, V] {

	case class Cursor(val key: K, val value: V, val node: LeafNode[K, V], val index: Int)

	private[rnkr] var head: LeafNode[K, V] = factory.data.newNode()
	private[rnkr] var root: Node[K] = head
	private[rnkr] var tail: LeafNode[K, V] = head
	private[rnkr] var _size: Int = 0
	private[rnkr] var leafCount: Int = 1
	private[rnkr] var indexCount: Int = 0
	private[rnkr] var level: Int = 1

	override def size = _size

	override def put(k: K, v: V): V = insert(k, v).value
	override def get(k: K): Option[V] = {
		val node = search(k)
		val index = node.indexOfKey(k)
		if (index >= 0) Some(node.childAt(index)) else None
	}

	override def keys(): Seq[K] = {
		val buf = new ListBuffer[K]
		def appendNode(node: LeafNode[K, V]): Unit = if (node != null) {
			buf ++= node.keys
			appendNode(node.next)
		}
		appendNode(head)
		buf.toList
	}

	override def keysReverse(): Seq[K] = {
		val buf = new ListBuffer[K]
		def appendNode(node: LeafNode[K, V]): Unit = if (node != null) {
			buf ++= node.keys.reverse
			appendNode(node.prev)
		}
		appendNode(tail)
		buf.toList
	}

	override def remove(k: K): Option[V] = delete(k) match {
		case Cursor(_, child, _, _) => Some(child)
		case _ => None
	}

	override def toString(): String = {
		def appendNode(buf: StringBuilder, node: Node[K]): Unit = {
			if (node != null) {
				node match {
					case n: IndexNode[K] => {
						var sep = ""
						buf.append("{")
						n.values zip n.keys foreach { i =>
							buf.append(sep)
							appendNode(buf, i._1)
							buf.append("<" + i._2 + " ")
							sep = " "
						}
						buf.append(">")
						appendNode(buf, n.values.last)
						buf.append("}");
					}
					case l: LeafNode[K, V] => {
						var sep = ""
						buf.append("[")
						l.keys zip l.values foreach { i =>
							buf.append(sep).append(i._1)
							sep = " "
						}
						buf.append("]");
					}
				}
			}
		}

		val buf = new StringBuilder()
		buf.append("{").append("size=").append(_size)
		appendNode(buf, root)
		buf.append("}").toString()

	}

	private def seek(k: K): Cursor = {
		val leaf = search(k)
		val index = leaf.indexOfKey(k)
		Cursor(k, leaf.childAt(index), leaf, index)
	}

	private def insert(k: K, value: V): Cursor = {
		val path = pathTo(k)
		val targetNode = path.head.asInstanceOf[LeafNode[K, V]]
		val index = targetNode.indexOfKey(k)

		def propagateToParent(key: K, a: Node[K], b: Node[K], parent: List[IndexNode[K]]): IndexNode[K] = {
			if (parent == Nil) {
				val newRoot = factory.index.newNode(Seq(key), Seq(a, b))
				root = newRoot
				level += 1
				indexCount += 1
				newRoot
			}
			else {
				val targetNode = parent.head
				val inserted = factory.index.insert(targetNode, key, b)
				if (inserted.split) {
					indexCount += 1
					propagateToParent(inserted.key, inserted.a, inserted.b, parent.tail)
				}
				else {
					inserted.a
				}
			}
		}

		if (index >= 0) {
			val updated = factory.data.update(targetNode, k, value)
			Cursor(k, updated.child, updated.node, index)
		}
		else {
			val inserted = factory.data.insert(targetNode, k, value)
			_size += 1
			if (inserted.split) {
				leafCount += 1
				inserted.b.next = targetNode.next
				inserted.a.next = inserted.b
				inserted.b.prev = inserted.a
				if (inserted.b.next != null) inserted.b.next.prev = inserted.b
				head = if (targetNode eq head) inserted.a else head
				tail = if (targetNode eq tail) inserted.b else tail
				propagateToParent(inserted.key, inserted.a, inserted.b, path.tail.asInstanceOf[List[IndexNode[K]]])
				Cursor(k, value, inserted.b, inserted.index)
			} else {
				Cursor(k, value, inserted.node, index)
			}
		}

	}

	private def delete(k: K): Cursor = {

		def deleteFromParent(path: Seq[IndexNode[K]], k: K): Node[K] = {
			val deleted = factory.index.delete(path.head, k)
			val node = deleted.a
			indexCount -= 1
			if (node.isEmpty) {
				root = node.childAt(0)
				level -= 1
			}
			else if (!factory.index.balanced(node)) {
				if (path.tail != Nil) {
					val parent = path.tail.head

					val index = parent.values.indexOf(node)
					val lv = parent.childOption(index - 1)
					val rv = parent.childOption(index + 1)

					val (av, bv) = (lv, rv) match {
						case (None, Some(rv)) => (node, rv)
						case (Some(lv), None) => (lv, node)
						case (Some(lv), Some(rv)) => if (lv.size > rv.size) (lv, node) else (node, rv)
						case (None, None) => throw new IllegalArgumentException
					}

					val initialSize = av.size
					val balanced = factory.index.redistribute(av.asInstanceOf[IndexNode[K]], Seq(bv.asInstanceOf[IndexNode[K]].values.head.keys.head), bv.asInstanceOf[IndexNode[K]])
					if (balanced.b.isEmpty)
						deleteFromParent(path.tail, balanced.key)
					else if (balanced.a.size != initialSize)
						factory.index.replaceKey(parent, balanced.key, balanced.b.keys.head)
				}
			}
			deleted.child
		}

		val path = pathTo(k)
		val targetNode = path.head.asInstanceOf[LeafNode[K, V]]

		val index = targetNode.indexOfKey(k)
		if (index >= 0) {
			val updated = factory.data.delete(targetNode, k)
			_size -= 1
			if (factory.data.balanced(updated.a) || updated.a == root)
				Cursor(k, updated.child, updated.node, index)
			else {
				val (av, bv) = (Option(updated.a.prev), Option(updated.a.next)) match {
					case (None, Some(rv)) => (updated.a, rv)
					case (Some(lv), None) => (lv, updated.a)
					case (Some(lv), Some(rv)) => if (lv.size > rv.size) (lv, updated.a) else (updated.a, rv)
					case (None, None) => throw new IllegalArgumentException
				}
				val initialSize = av.size
				val parent = path.tail.asInstanceOf[Seq[IndexNode[K]]]
				val balanced = factory.data.redistribute(av, Seq(), bv)
				if (balanced.b.isEmpty) {
					// special case for LeafNode
					balanced.a.next = balanced.b.next
					if (balanced.a.next != null) balanced.a.next.prev = balanced.a
					if (balanced.b eq tail) tail = balanced.a
					if (path.tail == Nil)
						root = path.head
					else
						deleteFromParent(parent, balanced.key)
				}
				else if (balanced.a.size != initialSize)
					factory.index.replaceKey(parent.head, balanced.key, balanced.b.keys.head)

				Cursor(k, balanced.child, balanced.node, balanced.index)
			}
		}
		else null

	}

	override def range(k: K, length: Int): Seq[Pair[K, V]] = {

		@tailrec def rangeForwards(buf: ListBuffer[Pair[K, V]], khead: Seq[K], vhead: Seq[V], next: LeafNode[K, V], leftover: Int) {
			if (leftover > 0) {
				if (khead == Nil) {
					if (next != null) rangeForwards(buf, next.keys, next.values, next.next, leftover)
				}
				else {
					buf.append((khead.head, vhead.head))
					rangeForwards(buf, khead.tail, vhead.tail, next, leftover - 1)
				}
			}
		}

		@tailrec def rangeBackwards(buf: ListBuffer[Pair[K, V]], khead: Seq[K], vhead: Seq[V], prev: LeafNode[K, V], leftover: Int) {
			if (leftover > 0) {
				if (khead == Nil) {
					if (prev != null) rangeBackwards(buf, prev.keys.reverse, prev.values.reverse, prev.prev, leftover)
				}
				else {
					buf.append((khead.head, vhead.head))
					rangeBackwards(buf, khead.tail, vhead.tail, prev, leftover - 1)
				}
			}
		}

		val leaf = search(k)
		val buf = new ListBuffer[Pair[K, V]]
		if (length >= 0) {
			val index = after(k, leaf.keys)
			rangeForwards(buf, leaf.keys.drop(index), leaf.values.drop(index), leaf.next, length)
		} else {
			val index = before(k, leaf.keys)
			rangeBackwards(buf, leaf.keys.take(index).reverse, leaf.values.take(index).reverse, leaf.prev, -length)
		}
		buf.toSeq
	}

	private def after(key: K, keys: Seq[K]): Int = keys.lastIndexWhere(factory.ordering.gt(key, _)) + 1
	private def before(key: K, keys: Seq[K]): Int = keys.lastIndexWhere(factory.ordering.ge(key, _)) + 1
	private def search(k: K): LeafNode[K, V] = pathTo(k, root, Nil, after).head.asInstanceOf[LeafNode[K, V]]
	private def pathTo(k: K): List[Node[K]] = pathTo(k, root, Nil, after)
	private def pathTo(k: K, n: Node[K], path: List[Node[K]], indexBound: (K, Seq[K]) => Int): List[Node[K]] = {
		n match {
			case l: LeafNode[K, V] => l :: path
			case c: IndexNode[K] => pathTo(k, c.childAt(indexBound(k, c.keys)), c :: path, indexBound)
		}
	}
}