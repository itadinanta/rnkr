package net.itadinanta.rnkr.tree

trait Ordering[T] {
	def lt(a: T, b: T): Boolean
	final def eq(a: T, b: T) = a == b
	final def le(a: T, b: T) = eq(a, b) || lt(a, b)
	final def gt(a: T, b: T) = !le(a, b)
	final def ge(a: T, b: T) = !lt(a, b)
	final def min(a: T, b: T): T = if (lt(a,b)) a else b
	final def max(a: T, b: T): T = if (lt(a,b)) b else a
}

trait Node[K] {
	def keys: Seq[K]
	def isEmpty: Boolean
	def indexOfKey(key: K) = keys.indexOf(key)
	def keyAt(index: Int) = keys(index)
	def keyOption(index: Int) = if (0 <= index && index < keys.length) Some(keyAt(index)) else None
}

trait Children[ChildType] {
	type Countable = Long
	def values: Seq[ChildType]
	def counts: Seq[Countable]
	def indexOfChild(child: ChildType) = values.indexOf(child)
	def childAt(index: Int) = values(index)
	def childOption(index: Int) = if (0 <= index && index < values.length) Some(childAt(index)) else None
	def countAt(index: Int) = counts(index)
}

trait DataNode[K, V] extends Node[K] with Children[V]

trait IndexNode[K] extends Node[K] with Children[Node[K]] {
	override def toString = {
		val buf = new StringBuilder
		var sep = ""
		buf.append("{")
		if (!isEmpty) {
			values zip keys foreach { i =>
				buf.append(sep)
				buf.append(i._1)
				buf.append("<" + i._2)
				sep = ">"
			}
			buf.append(">")
			buf.append(values.last)
		}
		buf.append("}");
		buf.toString
	}
}

trait LeafNode[K, V] extends DataNode[K, V] {
	var next: LeafNode[K, V]
	var prev: LeafNode[K, V]
	def childOfKey(key: K) = childAt(indexOfKey(key))
	override def toString = {
		val buf = new StringBuilder
		var sep = ""
		buf.append("[")
		keys zip values foreach { i =>
			buf.append(sep).append(i._1)
			sep = " "
		}
		buf.append("]");
		buf.toString
	}
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

