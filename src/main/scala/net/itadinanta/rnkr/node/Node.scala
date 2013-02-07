package net.itadinanta.rnkr.node

import collection.mutable.ArrayBuffer

trait NodeOrder[K] {
	type Order = (K, K) => Boolean
	type Comparator = (K, K) => Int
}

trait Node[K, +V] extends NodeOrder[K] {
	def apply(i: Int) = node(i)
	def apply: Option[V] = value
	def key(i: Int): Option[K]
	def node(i: Int): Option[Node[K, V]]
	def value: Option[V]

	def getKey(i: Int): K
	def getNode(i: Int): Node[K, V]

	def isLeaf: Boolean

	def count: Int
	def size: Int
	def keyCount = count - 1
	def createComparator(order: Order): Comparator = {
		(a: K, b: K) =>
			if (a == b) 0 else if (order(a, b)) 1 else -1
	}
}

class EmptyNode[K, V] extends Node[K, V] {
	override def key(i: Int) = None
	override def node(i: Int) = None
	override def value = None

	override def getKey(i: Int) = throw new NoSuchElementException
	override def getNode(i: Int) = throw new NoSuchElementException

	override def isLeaf = false

	override def count = 0
	override def size = 0
}

trait KeysContainer[K] extends NodeOrder[K] {
	val keys: Seq[K]
	def getKey(i: Int) = keys(i)
	def key(i: Int) = if (i >= 0 && i < keySize) Option(getKey(i)) else None

	def keySize: Int = keys.size

	def binarySearch(k: K, compare: Comparator): Int = binarySearch(k, compare, 0, keys.length)

	def binarySearch(k: K, compare: Comparator, a: Int, b: Int): Int = {
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

	def find(k: K): Int = {
		keys.zipWithIndex.find { p => p._1 == k } match {
			case Some((_, i)) => i
			case None => -1
		}
	}
}

trait MutableKeysContainer[K] extends KeysContainer[K] {
	override val keys = ArrayBuffer[K]()
	def setKey(i: Int, key: K) = keys(i) = key
	def removeKey(i: Int): K = keys.remove(i)
}

trait NodesContainer[K, +V] {
	val nodes: Seq[Node[K, V]]
	def getNode(i: Int) = nodes(i)
	def node(i: Int) = if (i >= 0 && i < nodeSize) Option(getNode(i)) else None
	def values = nodes.flatMap(_.value)
	def nodeSize = nodes.size
}

trait MutableNodesContainer[K, V] extends NodesContainer[K, V] {
	override val nodes = new ArrayBuffer[Node[K, V]]()
	def setNode(i: Int, node: Node[K, V]) = nodes(i) = node
	def removeNode(i: Int): Node[K, V] = nodes.remove(i)
	def updateNode(i: Int, node: Node[K, V]) = {
		if (i == nodes.length) {
			nodes += node;
		} else {
			nodes(i) = node
		}
		i
	}
}

abstract case class ContainerNode[K, +V] extends Node[K, V] with KeysContainer[K] with NodesContainer[K, V] {
	override def count = keys.length;
	override def value = None
	def pairs = keys.zip(values)
	override def size = nodes.map(_.size).fold(nodes.size) { _ + _ }
}

abstract class IndexNode[K, +V] extends ContainerNode[K, V] with NodesContainer[K, V] {
	override def getNode(i: Int) = nodes(i)
	override def isLeaf = false
}

abstract class LeafNode[K, +V] extends IndexNode[K, V] with NodesContainer[K, V] {
	override def isLeaf = true
}

case class DataNode[K, +V](val v: V) extends Node[K, V] {
	override def key(i: Int) = None
	override def node(i: Int) = None
	override val value = Some(v)

	override def getKey(i: Int) = throw new NoSuchElementException
	override def getNode(i: Int): Node[K, V] = throw new NoSuchElementException

	override def count = 0
	override def size = 1

	override def isLeaf = false
}

case class NodePointer[K, V](val node: Node[K, V], val key: K, val value: V, val index: Int) {
	assert(node.isLeaf)
}

trait MutableContainerNode[K, V] extends ContainerNode[K, V]
	with MutableKeysContainer[K]
	with MutableNodesContainer[K, V] {

	def copyFrom(source: ContainerNode[K, V]) = {
		nodes.clear()
		nodes ++= source.nodes
		keys.clear()
		keys ++= source.keys
		this
	}
}

class NodeBuilder[K: Manifest, V: Manifest](source: ContainerNode[K, V], order: (K, K) => Boolean)
	extends MutableContainerNode[K, V] {
	copyFrom(source)

	override def isLeaf = source.isLeaf

	def this(anOrder: (K, K) => Boolean) = this(new LeafNode[K, V] with MutableContainerNode[K, V], anOrder)

	val comparator = createComparator(order)

	override def size = 0

	def newDataNode(value: V): DataNode[K, V] = new DataNode[K, V](value)

	def newLeafNode: LeafNode[K, V] = {
		val node = new LeafNode[K, V] with MutableContainerNode[K, V]
		node.copyFrom(this)
		node
	}

	def newIndexNode: Node[K, V] = {
		val node = new IndexNode[K, V] with MutableContainerNode[K, V]
		node.copyFrom(this)
		node
	}

	def updateKeyAndValue(k: K, v: V): NodePointer[K, V] = NodePointer(this, k, v,
		keys.zipWithIndex.find((keyPair) => comparator(keyPair._1, k) <= 0) match {
			case Some((key, index)) => {
				if (k == key) {
					nodes(index) = DataNode(v)
					index
				} else {
					keys.insert(0, k)
					nodes.insert(0, DataNode(v))
					0
				}
			}
			case None => {
				-1
			}
		})
}

object HelloWorld {
	def run() = printf("Hello world!")
}
