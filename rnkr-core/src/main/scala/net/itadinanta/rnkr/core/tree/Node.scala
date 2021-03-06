package net.itadinanta.rnkr.core.tree

trait Ordering[T] {
	def lt(a: T, b: T): Boolean
	final def eq(a: T, b: T) = a == b
	final def le(a: T, b: T) = eq(a, b) || lt(a, b)
	final def gt(a: T, b: T) = !le(a, b)
	final def ge(a: T, b: T) = !lt(a, b)
	final def min(a: T, b: T): T = if (lt(a, b)) a else b
	final def max(a: T, b: T): T = if (lt(a, b)) b else a
}

object Rank {
	type Position = Long
}

import Rank.Position

trait Node[K] {
	def keys: Seq[K]
	def isEmpty: Boolean
	def count: Position
	def indexOfKey(key: K) = keys.indexOf(key)
	def keyAt(index: Int) = keys(index)
	def keyOption(index: Int) = if (0 <= index && index < keys.length) Some(keyAt(index)) else None
}

trait Children[ChildType] {
	def values: Seq[ChildType]
	def indexOfChild(child: ChildType) = values.indexOf(child)
	def childAt(index: Int) = values(index)
	def childOption(index: Int) = if (0 <= index && index < values.length) Some(childAt(index)) else None
}

case class Row[K, V](val key: K, val value: V, val rank: Position)

trait DataNode[K, V] extends Node[K] with Children[V] {
	override def count: Position = values.size
}

trait IndexNode[K] extends Node[K] with Children[Node[K]] {
	def partialRanks: Seq[Position]
	def partialRankAt(index: Int) = if (index < 0) 0L else partialRanks(index)
	override def count: Position = partialRanks.last
	def topLevel = {
		val buf = new StringBuilder
		var sep = ""
		buf.append("{")
		if (!isEmpty) {
			(values, keys, partialRanks).zipped.toList foreach { i =>
				buf.append(sep)
					.append(i._1.count)
					.append("(").append(i._3).append(")")
					.append("<" + i._2)
				sep = ">"
			}
			buf.append(">")
				.append(values.last.count)
				.append("(").append(partialRanks.last).append(")")
		}
		buf.append("}")
			.toString
	}
	override def toString = {
		val buf = new StringBuilder
		var sep = ""
		buf.append("{")
		if (!isEmpty) {
			(values, keys, partialRanks).zipped.toList foreach { i =>
				buf.append(sep)
					.append(i._1)
					.append("(").append(i._3).append(")")
					.append("<" + i._2)
				sep = ">"
			}
			buf.append(">")
				.append(values.last)
				.append("(").append(partialRanks.last).append(")")
		}
		buf.append("}")
			.toString
	}
}

trait LeafNode[K, V] extends DataNode[K, V] {
	var next: LeafNode[K, V]
	var prev: LeafNode[K, V]
	def childOfKey(key: K) = childAt(indexOfKey(key))
	override def toString = "[" + keys.mkString(" ") + "]"
}

object IntAscending extends Ordering[Int] {
	override def lt(a: Int, b: Int) = a < b
}

object LongAscending extends Ordering[Long] {
	override def lt(a: Long, b: Long) = a < b
}

object IntDescending extends Ordering[Int] {
	override def lt(a: Int, b: Int) = a > b
}

object StringAscending extends Ordering[String] {
	override def lt(a: String, b: String) = a < b
}

object StringDescending extends Ordering[String] {
	override def lt(a: String, b: String) = a > b
}

object StringCIAscending extends Ordering[String] {
	override def lt(a: String, b: String) = a.toLowerCase < b.toLowerCase
}

object StringCIDescending extends Ordering[String] {
	override def lt(a: String, b: String) = a.toLowerCase > b.toLowerCase
}

