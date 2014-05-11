package net.itadinanta.rnkr.node

trait Ordering[T] {
	def lt(a: T, b: T): Boolean
	final def eq(a: T, b: T) = a == b
	final def le(a: T, b: T) = eq(a, b) || lt(a, b)
	final def gt(a: T, b: T) = !le(a, b)
	final def ge(a: T, b: T) = !lt(a, b)
}

trait AbstractNode[K, V, ChildType] {
	val parent: IndexNode[K, V]
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

