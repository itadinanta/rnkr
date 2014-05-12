package net.itadinanta.rnkr.tree

import net.itadinanta.rnkr.node._
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

trait NodeBuilder[K, V, ChildType, NodeType <: Node[K] with Children[ChildType]] {
	val ordering: Ordering[K]
	val fanout: Int
	val splitOffset: Int
	def updateNode(input: NodeType, keys: Seq[K], children: Seq[ChildType]): NodeType
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

	private def merge(input: NodeType, aKeys: Seq[K], aValues: Seq[ChildType], bKeys: Seq[K], bValues: Seq[ChildType]) =
		new BuildResult(updateNode(input, aKeys ++ bKeys, aValues ++ bValues), bKeys.head, bValues.head, aKeys.size)

	private def insert(input: NodeType, keys: Seq[K], children: Seq[ChildType], k: K, child: ChildType) = {
		if (keys.isEmpty)
			new BuildResult(updateNode(input, Seq(k), Seq(child)), k, child, 0)
		else {
			val position = keys.lastIndexWhere(item => ordering.gt(k, item)) + 1
			val (keyHead, keyTail) = keys.splitAt(position)
			val (childHead, childTail) = children.splitAt(position + splitOffset)
			val newKeys = keyHead ++ Seq(k) ++ keyTail
			val newChildren = childHead ++ Seq(child) ++ childTail
			if (newKeys.length <= fanout) {
				new BuildResult(updateNode(input, newKeys, newChildren), k, child, position + splitOffset)
			} else {
				val splitAt = keys.size / 2
				val (newKeyHead, newKeyTail) = newKeys.splitAt(splitAt)
				val (newChildHead, newChildTail) = newChildren.splitAt(splitAt + splitOffset)
				BuildResult(updateNode(input, newKeyHead, newChildHead), newNode(if (splitOffset == 0) newKeyTail else newKeyTail.tail, newChildTail), newKeys(splitAt), children(splitAt + splitOffset), splitAt + splitOffset)
			}
		}
	}

	private def delete(input: NodeType, keys: Seq[K], children: Seq[ChildType], key: K) = {
		val position = keys.indexOf(key)
		if (position < 0) throw new IndexOutOfBoundsException("key " + key + " does not exist")

		new BuildResult(updateNode(
			input,
			keys.take(position) ++ keys.slice(position + 1, children.size),
			children.take(position) ++ children.slice(position + 1, children.size)),
			key, children(position), position)
	}

	private def update(input: NodeType, keys: Seq[K], children: Seq[ChildType], k: K, child: ChildType) = {
		val position = keys.indexOf(k)
		if (position < 0) throw new IndexOutOfBoundsException("key " + k + " does not exist")

		new BuildResult(updateNode(
			input,
			keys,
			children.take(position) ++ Seq(child) ++ children.slice(position + 1, children.size)),
			k, children(position), position)
	}
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
		override def updateNode(node: IndexNode[K], keys: Seq[K], children: Seq[Node[K]]) = node.asInstanceOf[SeqNodeImpl].set(keys, children)
	}

	override val data = new DataNodeBuilder {

		private[SeqNodeFactory] class SeqNodeImpl(var keys: Seq[K], var values: Seq[V]) extends LeafNode[K, V] {
			override var prev: LeafNode[K, V] = _
			override var next: LeafNode[K, V] = _

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
		override def updateNode(node: LeafNode[K, V], keys: Seq[K], children: Seq[V]) = node.asInstanceOf[SeqNodeImpl] set(keys, children)
	}
}

trait BPlusTree[K, V] {
	def size: Int
	def get(k: K): V
	def put(k: K, value: V): V
	def keys(): Seq[K]
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

	def size = _size

	def put(k: K, v: V): V = insert(k, v).value
	def get(k: K): V = search(k).childOfKey(k)
	def keys(): Seq[K] = {
		val buf = new ListBuffer[K]
		def appendNode(node: LeafNode[K, V]): Unit = if (node != null) {
			buf ++= node.keys
			appendNode(node.next)
		}
		appendNode(head)
		buf.toList
	}

	def keysReverse(): Seq[K] = {
		val buf = new ListBuffer[K]
		def appendNode(node: LeafNode[K, V]): Unit = if (node != null) {
			buf ++= node.keys.reverse
			appendNode(node.prev)
		}
		appendNode(tail)
		buf.toList
	}

	override def toString(): String = {
		val buf = new StringBuilder()
		buf.append("{").append("size=").append(_size)
		appendNode(buf, root)
		buf.append("}").toString()
	}

	private def appendNode(buf: StringBuilder, node: Node[K]): Unit = {
		if (node != null) {
			node match {
				case n: IndexNode[K] => {
					var sep = ""
					buf.append("{")
					n.values zip n.keys foreach { i =>
						buf.append(sep)
						appendNode(buf, i._1)
						buf.append("<-" + i._2 + " ")
						sep = " "
					}
					buf.append("->")
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

	private def insert(k: K, value: V): Cursor = {
		val path = pathTo(k)
		val targetNode = path.head.asInstanceOf[LeafNode[K, V]]
		val index = targetNode.indexOfKey(k)
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

	private def propagateToParent(key: K, a: Node[K], b: Node[K], parent: List[IndexNode[K]]): IndexNode[K] = {
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

	def seek(k: K): Cursor = {
		val leaf = search(k)
		val index = leaf.indexOfKey(k)
		Cursor(k, leaf.childAt(index), leaf, index)
	}

	private def search(k: K): LeafNode[K, V] = pathTo(k, root, Nil).head.asInstanceOf[LeafNode[K, V]]
	private def pathTo(k: K): List[Node[K]] = pathTo(k, root, Nil)
	private def pathTo(k: K, n: Node[K], path: List[Node[K]]): List[Node[K]] = {
		n match {
			case l: LeafNode[K, V] => l :: path
			case c: IndexNode[K] => pathTo(k, c.childAt(indexBound(k, c.keys)), c :: path)
		}
	}

	private def indexBound(key: K, keys: Seq[K]): Int = keys.lastIndexWhere(factory.ordering.gt(key, _)) + 1
}