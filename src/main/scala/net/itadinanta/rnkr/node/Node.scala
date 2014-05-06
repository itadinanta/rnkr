package net.itadinanta.rnkr.node

trait Ordering[T] {
	def lt(a: T, b: T): Boolean
	final def eq(a: T, b: T) = a == b
	final def le(a: T, b: T) = eq(a, b) || lt(a, b)
	final def gt(a: T, b: T) = !le(a, b)
	final def ge(a: T, b: T) = !lt(a, b)
}

trait Node {
	def size: Int
}

trait ChildNode extends Node

trait WithChildren[K, T <: ChildNode] {
	val keys: Seq[K]
	val children: Seq[T]
	def indexOfKey(key: K) = keys.indexOf(key)
	def indexOfChild(child: T) = children.indexOf(child)
	def childOfKey(key: K) = children(indexOfKey(key))
	def childAt(index: Int) = children(index)
	def keyAt(index: Int) = keys(index)
}

trait RootNode[K, T <: ChildNode] extends Node with WithChildren[K, T]

trait InternalNode[K, T <: ChildNode] extends ChildNode with WithChildren[K, T]

trait LeafNode[K, V] extends ChildNode with WithChildren[K, Record[V]] {
	val next: LeafNode[K, V]
	val prev: LeafNode[K, V]
}

trait Record[V] extends ChildNode {
	override def size = 1
	def data: V
}

trait RecordBuilder[K, V] {
	def newNode(key: K, data: V): WithChildren[K, Record[V]]
	def newRecord(data: V): Record[V]
}

trait ChildrenBuilder[K, T <: ChildNode] {
	def newNode(keys: Seq[K], children: Seq[T]): WithChildren[K, T]
	def emptyNode(): WithChildren[K, T]
	def insert(node: WithChildren[K, T], key: K, child: T): NodePointer[K, T]
	def delete(node: WithChildren[K, T], key: K): NodePointer[K, T]
	def update(node: WithChildren[K, T], key: K, child: T): NodePointer[K, T]
}

class NodePointer[K, V](val node: Node, val key: K, val value: V, val index: Int)

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

class NodeBuilder[K, V, T <: ChildNode](val ordering: Ordering[K]) extends ChildrenBuilder[K, T] with RecordBuilder[K, V] {

	private[NodeBuilder] class SimpleRecord[V](val data: V) extends Record[V]

	private[NodeBuilder] class WithChildrenSeq[K, U <: ChildNode](val keys: Seq[K], val children: Seq[U]) extends WithChildren[K, U] with Node {
		def this() = this(Seq[K](), Seq[U]())
		override val size = keys.length
	}

	override def emptyNode() = newNode(Seq[K](), Seq[T]())
	override def newNode(keys: Seq[K], children: Seq[T]) = new WithChildrenSeq[K, T](keys, children)
	override def newNode(key: K, data: V): WithChildren[K, Record[V]] =
		new WithChildrenSeq[K, Record[V]](Seq[K](key), Seq[Record[V]](newRecord(data)))
	override def newRecord(data: V): Record[V] = new SimpleRecord[V](data)

	def insertValue(node: WithChildren[K, T], k: K, value: V) = insert(node, k, newRecord(value))
	def updateValue(node: WithChildren[K, T], k: K, value: V) = update(node, k, newRecord(value))

	override def insert(node: WithChildren[K, T], k: K, child: T) = insert(node.keys, node.children, k, child)
	override def delete(node: WithChildren[K, T], k: K) = delete(node.keys, node.children, k)
	override def update(node: WithChildren[K, T], k: K, child: T) = update(node.keys, node.children, k, child)

	private def insert(keys: Seq[K], children: Seq[T], k: K, child: T): NodePointer[K, T] = {
		if (keys.isEmpty)
			new NodePointer[K, T](newNode(Seq[K](k), Seq[T](child)), k, child, 0)
		else {
			val position = keys.indexWhere(ordering.lt(k, _))
			new NodePointer[K, T](newNode(
				keys.take(position) ++ Seq(k) ++ keys.slice(position, keys.size),
				children.take(position) ++ Seq(child) ++ children.slice(position, children.size)),
				k, child, position)
		}
	}

	private def delete(keys: Seq[K], children: Seq[T], key: K): NodePointer[K, T] = {
		val position = keys.indexOf(key)
		if (position < 0) throw new IndexOutOfBoundsException("key " + key + " does not exist")

		new NodePointer[K, T](newNode(
			keys.take(position) ++ keys.slice(position + 1, children.size),
			children.take(position) ++ children.slice(position + 1, children.size)),
			key, children(position), position)
	}

	private def update(keys: Seq[K], children: Seq[T], k: K, child: T): NodePointer[K, T] = {
		val position = keys.indexOf(k)
		if (position < 0) throw new IndexOutOfBoundsException("key " + k + " does not exist")

		new NodePointer[K, T](newNode(
			keys,
			children.take(position) ++ Seq(child) ++ children.slice(position + 1, children.size)),
			k, children(position), position)
	}
}

object HelloWorld {
	def run() = printf("Hello world!")
}
