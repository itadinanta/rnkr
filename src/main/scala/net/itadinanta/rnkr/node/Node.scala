package net.itadinanta.rnkr.node

import scala.collection.mutable.ListBuffer

trait Ordering[T] {
	def lt(a: T, b: T): Boolean
	final def eq(a: T, b: T) = a == b
	final def le(a: T, b: T) = eq(a, b) || lt(a, b)
	final def gt(a: T, b: T) = !le(a, b)
	final def ge(a: T, b: T) = !lt(a, b)
}

trait Node[V] {
	val size: Int
	def values: Seq[V]
}

trait ChildNode[V] extends Node[V]

trait WithChildren[K, V] {
	val keys: Seq[K]
	val children: Seq[ChildNode[V]]
	def values: Seq[V]
	def indexOfKey(key: K) = keys.indexOf(key)
	def indexOfChild(child: ChildNode[V]) = children.indexOf(child)
	def childOfKey(key: K) = children(indexOfKey(key))
	def childAt(index: Int) = children(index)
	def keyAt(index: Int) = keys(index)
}

trait RootNode[K, V] extends Node[V] with WithChildren[K, V]

trait InternalNode[K, V] extends ChildNode[V] with WithChildren[K, V]

trait LeafNode[K, V] extends ChildNode[V] with WithChildren[K, V] {
	val next: LeafNode[K, V]
	val prev: LeafNode[K, V]
}

trait Record[V] extends ChildNode[V] {
	override val size = 1
	val data: V
	override def values: Seq[V] = Seq(data)
}

trait RecordBuilder[K, V] {
	def newNode(key: K, data: V): WithChildren[K, V]
	def newRecord(data: V): Record[V]
}

trait ChildrenBuilder[K, V] {
	def newNode(keys: Seq[K], children: Seq[ChildNode[V]]): WithChildren[K, V]
	def emptyNode(): WithChildren[K, V]
	def insert(node: WithChildren[K, V], key: K, child: ChildNode[V]): NodeRef[K, V]
	def delete(node: WithChildren[K, V], key: K): NodeRef[K, V]
	def update(node: WithChildren[K, V], key: K, child: ChildNode[V]): NodeRef[K, V]
}

class NodeRef[K, V](val node: WithChildren[K, V], val key: K, val child: ChildNode[V], val index: Int)

class NodePointer[K, V](val node: WithChildren[K, V], val key: K, val value: V, val index: Int)

object IntAscending extends Ordering[Int] {
	override def lt(a: Int, b: Int): Boolean = a < b
}

object IntDescending extends Ordering[Int] {
	override def lt(a: Int, b: Int): Boolean = a > b
}

object StringAscending extends Ordering[String] {
	override def lt(a: String, b: String): Boolean = a < b
}

object StringDescending extends Ordering[String] {
	override def lt(a: String, b: String): Boolean = a > b
}

object StringCIAscending extends Ordering[String] {
	override def lt(a: String, b: String): Boolean = a.toLowerCase < b.toLowerCase
}

object StringCIDescending extends Ordering[String] {
	override def lt(a: String, b: String): Boolean = a.toLowerCase > b.toLowerCase
}

abstract class NodeBuilder[K, V](val ordering: Ordering[K]) extends ChildrenBuilder[K, V] with RecordBuilder[K, V] {

	override def emptyNode() = newNode(Seq[K](), Seq[ChildNode[V]]())

	def insertValue(node: WithChildren[K, V], k: K, value: V): NodeRef[K, V] = insert(node, k, newRecord(value))
	def updateValue(node: WithChildren[K, V], k: K, value: V): NodeRef[K, V] = update(node, k, newRecord(value))

	override def insert(node: WithChildren[K, V], k: K, child: ChildNode[V]): NodeRef[K, V] = insert(node.keys, node.children, k, child)
	override def delete(node: WithChildren[K, V], k: K): NodeRef[K, V] = delete(node.keys, node.children, k)
	override def update(node: WithChildren[K, V], k: K, child: ChildNode[V]): NodeRef[K, V] = update(node.keys, node.children, k, child)

	private def insert(keys: Seq[K], children: Seq[ChildNode[V]], k: K, child: ChildNode[V]): NodeRef[K, V] = {
		if (keys.isEmpty)
			new NodeRef(newNode(Seq(k), Seq(child)), k, child, 0)
		else {
			val position = keys.indexWhere(ordering.gt(k, _)) + 1
			new NodeRef(newNode(
				keys.take(position) ++ Seq(k) ++ keys.slice(position, keys.size),
				children.take(position) ++ Seq(child) ++ children.slice(position, children.size)),
				k, child, position)
		}
	}

	private def delete(keys: Seq[K], children: Seq[ChildNode[V]], key: K): NodeRef[K, V] = {
		val position = keys.indexOf(key)
		if (position < 0) throw new IndexOutOfBoundsException("key " + key + " does not exist")

		new NodeRef(newNode(
			keys.take(position) ++ keys.slice(position + 1, children.size),
			children.take(position) ++ children.slice(position + 1, children.size)),
			key, children(position), position)
	}

	private def update(keys: Seq[K], children: Seq[ChildNode[V]], k: K, child: ChildNode[V]): NodeRef[K, V] = {
		val position = keys.indexOf(k)

		if (position < 0) throw new IndexOutOfBoundsException("key " + k + " does not exist")

		new NodeRef(newNode(
			keys,
			children.take(position) ++ Seq(child) ++ children.slice(position + 1, children.size)),
			k, children(position), position)
	}
}

class ListNodeBuilder[K, V](ordering: Ordering[K]) extends NodeBuilder[K,V](ordering) {

	private[ListNodeBuilder] class SimpleRecord[V](val data: V) extends Record[V]

	private[ListNodeBuilder] class WithChildrenSeq[K, V](val keys: Seq[K], val children: Seq[ChildNode[V]]) extends WithChildren[K, V] with Node[V] {
		def this() = this(Seq[K](), Seq[ChildNode[V]]())
		override val size = keys.length
		override def values: Seq[V] = {
			val buf = new ListBuffer[V]
			children foreach { child => buf.appendAll(child.values)}
			buf
		}
	}

	override def newNode(keys: Seq[K], children: Seq[ChildNode[V]]) = new WithChildrenSeq(keys, children)
	override def newNode(key: K, data: V) = new WithChildrenSeq(Seq(key), Seq(newRecord(data)))
	override def newRecord(data: V): Record[V] = new SimpleRecord[V](data)
}


object HelloWorld {
	def run() = printf("Hello world!")
}
