package net.itadinanta.rnkr.node

trait Ordering[T] {
	def lt(a: T, b: T): Boolean
	final def eq(a: T, b: T) = a == b
	final def le(a: T, b: T) = eq(a, b) || lt(a, b)
	final def gt(a: T, b: T) = !le(a, b)
	final def ge(a: T, b: T) = !lt(a, b)
}

trait AbstractNode[K, V, ChildType] {
	val keys: Seq[K]
	val values: Seq[ChildType]
	def size: Int = keys.length
	def indexOfKey(key: K) = keys.indexOf(key)
	def indexOfChild(child: ChildType) = values.indexOf(child)
	def childOfKey(key: K) = values(indexOfKey(key))
	def childAt(index: Int) = values(index)
	def keyAt(index: Int) = keys(index)
}

trait Node[K, V] extends AbstractNode[K, V, Node[K, V]]

trait DataNode[K, V] extends AbstractNode[K, V, V]

trait IndexNode[K, V] extends Node[K, V]

trait LeafNode[K, V] extends DataNode[K, V] {
	val next: LeafNode[K, V]
	val prev: LeafNode[K, V]
}

trait NodeBuilder[K, V, ChildType, NodeType <: AbstractNode[K, V, ChildType]] {
	val ordering: Ordering[K]
	def newNode(keys: Seq[K], children: Seq[ChildType]): NodeType
	def emptyNode(): NodeType

	case class Ref(val node: NodeType, val key: K, val child: ChildType, val index: Int)

	def insert(node: NodeType, k: K, child: ChildType): Ref = insert(node.keys, node.values, k, child)
	def delete(node: NodeType, k: K): Ref = delete(node.keys, node.values, k)
	def update(node: NodeType, k: K, child: ChildType): Ref = update(node.keys, node.values, k, child)

	private def insert(keys: Seq[K], children: Seq[ChildType], k: K, child: ChildType) = {
		if (keys.isEmpty)
			Ref(newNode(Seq(k), Seq(child)), k, child, 0)
		else {
			val position = keys.indexWhere(ordering.gt(k, _)) + 1
			Ref(newNode(
				keys.take(position) ++ Seq(k) ++ keys.slice(position, keys.size),
				children.take(position) ++ Seq(child) ++ children.slice(position, children.size)),
				k, child, position)
		}
	}

	private def delete(keys: Seq[K], children: Seq[ChildType], key: K) = {
		val position = keys.indexOf(key)
		if (position < 0) throw new IndexOutOfBoundsException("key " + key + " does not exist")

		Ref(newNode(
			keys.take(position) ++ keys.slice(position + 1, children.size),
			children.take(position) ++ children.slice(position + 1, children.size)),
			key, children(position), position)
	}

	private def update(keys: Seq[K], children: Seq[ChildType], k: K, child: ChildType) = {
		val position = keys.indexOf(k)

		if (position < 0) throw new IndexOutOfBoundsException("key " + k + " does not exist")

		Ref(newNode(
			keys,
			children.take(position) ++ Seq(child) ++ children.slice(position + 1, children.size)),
			k, children(position), position)
	}
}

trait DataBuilder[K, V] extends NodeBuilder[K, V, V, DataNode[K, V]]

trait IndexBuilder[K, V] extends NodeBuilder[K, V, Node[K, V], Node[K, V]]

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

class SeqNodeBuilder[K, V](val ordering: Ordering[K] = IntAscending) {

	object Index extends IndexBuilder[K, V] {

		private[Index] class Impl[K, V](val keys: Seq[K], val values: Seq[Node[K, V]]) extends Node[K, V]

		override val ordering = SeqNodeBuilder.this.ordering
		override def emptyNode() = new Impl(Seq[K](), Seq[Node[K, V]]())
		override def newNode(keys: Seq[K], children: Seq[Node[K, V]]) = new Impl(keys, children)
	}

	object Data extends DataBuilder[K, V] {
		private[Data] class Impl[K, V](val keys: Seq[K], val values: Seq[V]) extends DataNode[K, V]
		override val ordering = SeqNodeBuilder.this.ordering

		def newNode(k: K, v: V) = new Impl(Seq(k), Seq(v))
		override def emptyNode() = new Impl(Seq[K](), Seq[V]())
		override def newNode(keys: Seq[K], children: Seq[V]) = new Impl(keys, children)
	}

}

object HelloWorld {
	def run() = printf("Hello world!")
}
