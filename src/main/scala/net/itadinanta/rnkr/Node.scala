package net.itadinanta.rnkr.node

import collection.mutable.ArrayBuffer

abstract class Node[K, V] {
	def apply(i: Int) = node(i)
	def key(i: Int): Option[K]
	def node(i: Int): Option[Node[K, V]]
	def value(i: Int): Option[V]
	def valueForKey(i: K): Option[V]

	def getKey(i: Int): K
	def getNode(i: Int): Node[K, V]
	def getValue(i: Int): V

	def nodes: Seq[Node[K, V]]
	def keys: Seq[K]
	def values: Seq[V]
	def count: Int
	def size: Int
	def keyCount = count - 1
	def next: Option[Node[K, V]]
	def prev: Option[Node[K, V]]
}

case class Leaf[K, V](private val key: K, private val value: V) extends Node[K, V] {
	def key(i: Int) = if (i == 0) Option(key) else None
	def node(i: Int) = None
	def valueForKey(k: K) = if (k == key) Option(value) else None
	def value(i: Int) = if (i == 0) Option(value) else None

	def getKey(i: Int) = if (i == 0) key else throw new NoSuchElementException
	def getNode(i: Int): Node[K, V] = throw new NoSuchElementException
	def getValue(i: Int): V = if (i == 0) value else throw new NoSuchElementException

	def count = 0
	def size = 1
	def next = None
	def prev = None
	def nodes = Seq.empty[Node[K, V]]
	def keys = Seq.empty[K]
	def values = Seq.empty[V]
}

abstract case class ContainerNode[K, V] extends Node[K, V] {
	val nodes: Seq[Node[K, V]]
	val keys: Seq[K]
	val values: Seq[V]
	def prev: Option[Node[K, V]]
	def next: Option[Node[K, V]]

	def count = nodes.length;

	def getNode(i: Int) = nodes(i)
	def getKey(i: Int) = keys(i)
	def getValue(i: Int) = values(i)

	def node(i: Int) = if (i >= 0 && i < count) Option(nodes(i)) else None
	def key(i: Int) = if (i >= 0 && i < keyCount) Option(keys(i)) else None
	def value(i: Int) = if (i >= 0 && i < keyCount) Option(values(i)) else None
	def valueForKey(k: K) = {
		val i = getIndex(k)
		if (i >= 0) Option(values(i)) else None
	}

	def binarySearch(k: K, compare: (K, K) => Int): Int = binarySearch(k, compare, 0, keys.length)

	def binarySearch(k: K, compare: (K, K) => Int, a: Int, b: Int): Int = {
		if (a >= b) {
			-1
		} else {
			val c = (a + b) / 2
			val cmpz = compare(k, keys(c))
			if (cmpz == 0) c
			else if (cmpz < 0) binarySearch(k, compare, a, c - 1)
			else binarySearch(k, compare, c + 1, a)
		}
	}

	def getIndex(k: K): Int = find(k)

	def find(k: K): Int = {
		keys.zipWithIndex.find { p => p._1 == k } match {
			case Some((_, i)) => i
			case None => -1
		}
	}
}

case class ImmutableNode[K, V](
	val nodes: Seq[Node[K, V]],
	val keys: Seq[K],
	val values: Seq[V],
	val prev: Option[Node[K, V]] = None,
	val next: Option[Node[K, V]] = None) extends ContainerNode[K, V] {
	val size = nodes.map(_.size).fold(nodes.length)(_ + _)
}

case class NodePointer[K, V](val node: Node[K, V], val key: K, val value: V, val index: Int)

case class NodeBuilder[K: Manifest, V: Manifest](source: Node[K, V], val comparator: (K, K) => Int) extends ContainerNode[K, V] {
	val nodes = ArrayBuffer(source.nodes: _*)
	val keys = ArrayBuffer(source.keys: _*)
	val values = ArrayBuffer(source.values: _*)

	var prev: Option[Node[K, V]] = source.prev
	var next: Option[Node[K, V]] = source.next

	var size = 0

	def setNode(i: Int, node: Node[K, V]) { nodes(i) = node }
	def setKeyAndValue(i: Int, k: K, v: V) { keys(i) = k; values(i) = v }

	def getValueForKey(k: K) = {
		val i = getIndex(k)
		if (i >= 0) Option(values(i)) else None
	}

	def copyFrom(source: Node[K, V]) = {
		nodes.clear()
		nodes ++= source.nodes
		keys.clear()
		keys ++= source.keys
		values.clear()
		values ++= source.values
		prev = source.prev
		next = source.next

		this
	}

	def newNode(): Node[K, V] = new ImmutableNode[K, V](nodes.toArray[Node[K, V]], keys.toArray[K], values.toArray[V], prev, next)

	def newLeaf(k: K, v: V): Node[K, V] = new Leaf(k, v)

	def removeNode(i: Int): Node[K, V] = nodes.remove(i)
	def removeKeyAndValue(i: Int): (K, V) = (keys.remove(i), values.remove(i))
	def removeKeyAndValue(k: K): (K, V) = removeKeyAndValue(getIndex(k))

	def updateNode(i: Int, node: Node[K, V]) = {
		if (i == nodes.length) {
			nodes += node;
		} else {
			nodes(i) = node
		}
		i
	}

	def updateKeyAndValue(k: K, v: V): NodePointer[K, V] = NodePointer(this, k, v,
		keys.zipWithIndex.find((keyPair) => comparator(keyPair._1, k) <= 0) match {
			case Some((key, index)) => {
				if (k == key) {
					values(index) = v
					index
				} else {
					keys.insert(0, k)
					values.insert(0, v)
					0
				}
			}
			case None => {
				keys += k;
				values += v;
				keys.length - 1
			}
		})

}

