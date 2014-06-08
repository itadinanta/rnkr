package net.itadinanta.rnkr.tree

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import Rank.Position

import org.slf4j.LoggerFactory

trait BPlusTree[K, V] {
	def size: Int
	def get(k: K): Option[Row[K, V]]
	def remove(k: K): Option[Row[K, V]]
	def put(k: K, value: V): Row[K, V]
	def append(k: K, value: V): Row[K, V]
	def keys(): Seq[K]
	def keysReverse(): Seq[K]
	def rank(k: K): Position
	def range(k: K, length: Int): Seq[Row[K, V]]
	def page(start: Position, length: Int): Seq[Row[K, V]]
}

class SeqBPlusTree[K, V](val factory: NodeFactory[K, V]) extends BPlusTree[K, V] {
	val LOG = LoggerFactory.getLogger(this.getClass())
	case class Cursor(val key: K, val value: V, val node: LeafNode[K, V], val index: Position)

	private[rnkr] var head: LeafNode[K, V] = factory.data.newNode()
	private[rnkr] var root: Node[K] = head
	private[rnkr] var tail: LeafNode[K, V] = head
	private[rnkr] var valueCount: Int = 0
	private[rnkr] var leafCount: Int = 1
	private[rnkr] var indexCount: Int = 0
	private[rnkr] var level: Int = 1

	override def size = valueCount
	override def rank(k: K): Position = {
		@tailrec def rank(n: Node[K], p: Position): Position = {
			n match {
				case l: LeafNode[K, V] => {
					val i = l.indexOfKey(k)
					if (i >= 0) p + i
					else if (p == 0) i
					else p + l.count
				}
				case c: IndexNode[K] => {
					val index = before(k, c.keys)
					rank(c.childAt(index), if (index == 0) 0: Position else c.partialRanks(index - 1))
				}
			}
		}
		rank(root, 0)
	}

	override def append(k: K, v: V) = {
		val inserted = insertAtEnd(k, v)
		Row(k, inserted.value, size)
	}

	override def put(k: K, v: V) = {
		val inserted = insert(k, v)
		Row(k, v, inserted.index)
	}

	override def get(k: K) = {
		val node = searchByKey(k)
		val index = node.indexOfKey(k)
		if (index >= 0) {
			val value = node.childAt(index)
			Some(Row(k, value, index))
		} else None
	}

	override def keys() = {
		val buf = new ListBuffer[K]
		def appendNode(node: LeafNode[K, V]): Unit = if (node != null) {
			buf ++= node.keys
			appendNode(node.next)
		}
		appendNode(head)
		buf.toList
	}

	override def keysReverse() = {
		val buf = new ListBuffer[K]
		def appendNode(node: LeafNode[K, V]): Unit = if (node != null) {
			buf ++= node.keys.reverse
			appendNode(node.prev)
		}
		appendNode(tail)
		buf.toList
	}

	override def remove(k: K) = delete(k) match {
		case Cursor(key, child, _, index) => Some(Row(key, child, index))
		case _ => None
	}

	override def toString(): String = {
		val buf = new StringBuilder()
		buf.append("{").append("size=").append(valueCount).append("/").append(leafCount).append("/").append(indexCount)
		buf.append(root)
		buf.append("}").toString()
	}

	def count(node: Node[K]): Position = node match {
		case data: LeafNode[K, V] => data.keys.size
		case index: IndexNode[K] => index.values.foldLeft(0: Position) { (a, b) => a + count(b) }
	}

	def min(node: Node[K]): K = node match {
		case data: LeafNode[K, V] => data.keys.reduceLeft { factory.data.ordering.min }
		case index: IndexNode[K] => index.values map (min(_)) reduceLeft { factory.data.ordering.min }
	}

	def first(node: Node[K]): K = node match {
		case data: LeafNode[K, V] => data.keys.head
		case index: IndexNode[K] => first(index.values.head)
	}

	def consistent(node: Node[K]): Boolean = node match {
		case data: LeafNode[K, V] =>
			(data.keys.size == data.values.size) &&
				((data.next eq null) && (tail eq data)) || ((data.next ne null) && (data.next.prev eq data)) &&
				((data.prev eq null) && (head eq data)) || ((data.prev ne null) && (data.prev.next eq data))

		case index: IndexNode[K] => {
			val indexKeys = index.keys
			val indexValues = index.values
			val indexPartialRanks = index.partialRanks
			(indexKeys.size == (indexValues.size - 1)) &&
				(index.isEmpty ||
					(factory.index.ordering.lt(min(indexValues.head), indexKeys.head) &&
						indexKeys.zip(indexValues.tail.map(min)).foldLeft(true) { (a, b) => a && (b._1 == b._2) } &&
						indexPartialRanks.zip(indexValues.map(count).scanLeft(0: Position)(_ + _).tail).foldLeft(true) { (a, b) => a && (b._1 == b._2) } &&
						indexValues.foldLeft(true) { (a, b) => (a && consistent(b)) }))
		}
	}

	def consistent: Boolean = consistent(root)

	private def seekByKey(k: K): Cursor = {
		val leaf = searchByKey(k)
		val index = leaf.indexOfKey(k)
		Cursor(k, leaf.childAt(index), leaf, index)
	}

	private def insertAtEnd(k: K, v: V): Cursor = {
		case class AppendedResult(a: Node[K], key: K, b: Option[Node[K]], grow: Position, cursor: Cursor)

		def appendRecursively(targetNode: Node[K], k: K, v: V): AppendedResult = targetNode match {
			case leaf: LeafNode[K, V] => {
				if (leaf.isEmpty || factory.ordering.gt(k, leaf.keys.last)) {
					val appended = factory.data.append(leaf, k, v)
					valueCount += 1
					val cursor = Cursor(k, v, leaf, appended.index)
					if (appended.split) {
						appended.b.next = appended.a.next
						appended.a.next = appended.b
						tail = appended.b
						tail.prev = appended.a
						leafCount += 1
						AppendedResult(appended.a, appended.key, Some(appended.b), +1, cursor)
					} else
						AppendedResult(appended.a, appended.key, None, +1, cursor)
				} else throw new IllegalArgumentException("Key " + k + " out of order")
			}
			case index: IndexNode[K] => {
				if (factory.ordering.gt(k, index.keys.last)) {
					appendRecursively(index.values.last, k, v) match {
						case AppendedResult(a, newKey, None, grow, cursor) =>
							AppendedResult(factory.index.grow(index, index.values.size - 1, grow), newKey, None, grow, cursor)
						case AppendedResult(a, newKey, Some(b), grow, cursor) => {
							val inserted = factory.index.append(index, newKey, b, 1)
							if (inserted.split) {
								indexCount += 1
								AppendedResult(inserted.a, inserted.key, Some(factory.index.grow(inserted.b, inserted.b.values.size - 1, grow)), grow, cursor)
							} else
								AppendedResult(factory.index.grow(inserted.a, inserted.a.values.size - 1, grow),
									inserted.key, None, grow, cursor)
						}
					}
				} else throw new IllegalArgumentException("Key " + k + " out of order")
			}
		}

		val appended = appendRecursively(root, k, v)
		root = appended match {
			case AppendedResult(a, key, Some(b), cursor, grow) => newRoot(key, a, b)
			case _ => root
		}

		appended.cursor
	}

	private def insert(k: K, v: V): Cursor = {
		case class InsertedResult(a: Node[K], aGrow: Int, key: K, b: Option[Node[K]], bGrow: Int, cursor: Cursor)

		def insertRecursively(targetNode: Node[K], k: K, v: V): InsertedResult = targetNode match {
			case leaf: LeafNode[K, V] => {
				val i = targetNode.indexOfKey(k)
				if (i >= 0)
					InsertedResult(leaf, 0, k, None, 0, Cursor(k, v, leaf, factory.data.update(leaf, k, v).index))
				else {
					val inserted = factory.data.insert(leaf, k, v, 1)
					valueCount += 1
					val cursor = Cursor(k, v, leaf, inserted.index)
					if (inserted.split) {
						inserted.b.next = leaf.next
						inserted.a.next = inserted.b
						inserted.b.prev = inserted.a
						if (inserted.b.next != null) inserted.b.next.prev = inserted.b
						head = if (leaf eq head) inserted.a else head
						tail = if (leaf eq tail) inserted.b else tail
						leafCount += 1
						val aGrow = if (inserted.index < inserted.a.keys.size) +1 else 0
						InsertedResult(inserted.a, aGrow, inserted.key, Some(inserted.b), 1 - aGrow, cursor)
					} else
						InsertedResult(inserted.a, +1, inserted.key, None, 0, cursor)
				}
			}
			case index: IndexNode[K] => {
				val i = index.keys.lastIndexWhere(factory.ordering.ge(k, _)) + 1
				insertRecursively(index.childAt(i), k, v) match {
					case InsertedResult(a, aGrow, newKey, None, bGrow, cursor) => {
						InsertedResult(factory.index.grow(index, i, aGrow), aGrow, newKey, None, 0, cursor)
					}
					case InsertedResult(a, aGrow, newKey, Some(b), bGrow, cursor) => {
						val inserted = factory.index.insert(index, newKey, b, 1)
						if (inserted.split) {
							indexCount += 1
							InsertedResult(factory.index.grow(inserted.a, i, aGrow), aGrow, inserted.key,
								Some(factory.index.grow(inserted.b, i + 1, bGrow)), bGrow, cursor)
						} else InsertedResult(factory.index.grow(index, i, aGrow), aGrow, inserted.key, None, 0, cursor)
					}
				}
			}
		}

		val inserted = insertRecursively(root, k, v)
		root = inserted match {
			case InsertedResult(a, aGrow, key, Some(b), bGrow, cursor) => newRoot(key, a, b)
			case _ => root
		}

		inserted.cursor
	}

	private def newRoot(key: K, a: Node[K], b: Node[K]) = {
		level += 1
		indexCount += 1
		factory.index.newNode(Seq(key), Seq(a, b), Seq(a.count, a.count + b.count))
	}

	private def delete(k: K): Cursor = {
		case class DeletedResult(pos: Int,
			ak: K,
			oldAk: K,
			a: Node[K],
			bk: Option[K],
			oldBk: Option[K],
			cursor: Cursor)

		def deleteRecursively(k: K, pos: Int, left: Option[Node[K]], targetNode: Node[K], right: Option[Node[K]]): DeletedResult = {
			// LOG.debug("Removing {} from {}", k, targetNode)
			// choose siblings
			lazy val (i, av, bv) = (left, right) match {
				case (None, Some(rv)) => (pos, targetNode, rv)
				case (Some(lv), None) => (pos - 1, lv, targetNode)
				case (Some(lv), Some(rv)) => if (lv.keys.size > rv.keys.size) (pos - 1, lv, targetNode) else (pos, targetNode, rv)
				case (None, None) => (pos, targetNode, targetNode)
			}

			targetNode match {
				case leaf: LeafNode[K, V] => {
					val leafK = leaf.keys.head
					val deleted = factory.data.delete(leaf, k)
					valueCount -= 1
					val cursor = Cursor(k, deleted.child, leaf, deleted.index)
					if (factory.data.balanced(leaf) || (av eq bv))
						DeletedResult(i, if (leaf.isEmpty) k else leaf.keyAt(0), leafK, leaf, None, None, cursor)
					else (av, bv) match {
						case (a: LeafNode[K, V], b: LeafNode[K, V]) => {
							val oldAk = if (a eq leaf) leafK else a.keys.head
							val oldBk = if (b eq leaf) leafK else b.keys.head
							val balanced = factory.data.redistribute(a, Seq(), b)

							if (balanced.b.isEmpty) {
								// special case for LeafNode
								balanced.a.next = balanced.b.next
								if (balanced.a.next != null) balanced.a.next.prev = balanced.a
								if (balanced.b eq tail) tail = balanced.a
								leafCount -= 1
							}
							DeletedResult(i, balanced.a.keyAt(0), oldAk, balanced.a, balanced.b.keyOption(0), Some(oldBk), cursor)
						}
					}
				}
				case index: IndexNode[K] => {
					val sub = index.keys.lastIndexWhere(factory.ordering.ge(k, _)) + 1
					deleteRecursively(k, sub, index.childOption(sub - 1), index.childAt(sub), index.childOption(sub + 1)) match {
						case DeletedResult(u, ak, oldAk, a, None, Some(oldPivot), cursor) => {
							if (sub > 0 && index.keyAt(sub - 1) == oldAk) factory.index.renameKeyAt(index, sub - 1, ak)
							val deleted = factory.index.deleteAt(index, u)
							if (factory.data.balanced(index) || (av eq bv)) {
								DeletedResult(pos, ak, oldAk, index, None, None, cursor)
							} else (av, bv) match {
								case (a: IndexNode[K], b: IndexNode[K]) => {
									val pivot = first(b)
									val oldBk = if (b eq index) oldPivot else b.keys.head
									val balanced = factory.index.redistribute(a, Seq(pivot), b)
									if (balanced.b.isEmpty) {
										indexCount -= 1
										DeletedResult(i, ak, oldAk, balanced.a, None, Some(pivot), cursor)
									} else {
										DeletedResult(i, ak, oldAk, balanced.a, Some(balanced.key), Some(pivot), cursor)
									}
								}
							}
						}

						case DeletedResult(u, ak, oldAk, a, Some(bk), oldBk, cursor) => {
							if (ak != oldAk && sub > 0 && index.keyAt(sub - 1) == oldAk) factory.index.renameKeyAt(index, sub - 1, ak)
							oldBk foreach { k =>
								if (bk != k) {
									if (sub > 0 && index.keyAt(sub - 1) == k) factory.index.renameKeyAt(index, sub - 1, bk)
									else if (index.keyAt(sub) == k) factory.index.renameKeyAt(index, sub, bk)
								}
							}
							DeletedResult(sub - 1, ak, oldAk, index, None, None, cursor)
						}

						case DeletedResult(u, k, oldK, node, None, None, cursor) => {
							if (k != oldK && sub > 0 && index.keyAt(sub - 1) == oldK) factory.index.renameKeyAt(index, sub - 1, k)
							DeletedResult(sub - 1, k, oldK, index, None, None, cursor)
						}
					}
				}
			}
		}

		deleteRecursively(k, 0, None, root, None) match {
			case DeletedResult(_, _, _, _, _, _, cursor) => {
				root = root match {
					case index: IndexNode[K] => if (index.isEmpty) {
						indexCount -= 1;
						level -= 1;
						index.childAt(0)
					} else root
					case _ => root
				}
				cursor
			}
		}
	}

	@tailrec private def rangeForwards(buf: ListBuffer[Row[K, V]], position: Position, khead: Seq[K], vhead: Seq[V], next: LeafNode[K, V], leftover: Int) {
		if (leftover > 0) {
			if (khead == Nil) {
				if (next != null) rangeForwards(buf, position, next.keys, next.values, next.next, leftover)
			} else {
				buf.append(Row(khead.head, vhead.head, position))
				rangeForwards(buf, position + 1, khead.tail, vhead.tail, next, leftover - 1)
			}
		}
	}

	override def page(start: Position, length: Int): Seq[Row[K, V]] = {
		val cursor: Cursor = searchByRank(start)
		if (cursor == null) Seq()
		else {
			val leaf = cursor.node
			val buf = new ListBuffer[Row[K, V]]
			val index = cursor.index.intValue
			rangeForwards(buf, start, leaf.keys.drop(index), leaf.values.drop(index), leaf.next, length)
			buf.toSeq
		}
	}

	@tailrec private def rangeBackwards(buf: ListBuffer[Row[K, V]], position: Position, khead: Seq[K], vhead: Seq[V], prev: LeafNode[K, V], leftover: Int) {
		if (leftover > 0) {
			if (khead == Nil) {
				if (prev != null) rangeBackwards(buf, position, prev.keys.reverse, prev.values.reverse, prev.prev, leftover)
			} else {
				buf.append(Row(khead.head, vhead.head, position))
				rangeBackwards(buf, position - 1, khead.tail, vhead.tail, prev, leftover - 1)
			}
		}
	}

	override def range(k: K, length: Int): Seq[Row[K, V]] = {
		val leaf = searchByKey(k)
		val buf = new ListBuffer[Row[K, V]]
		if (length >= 0) {
			val index = after(k, leaf.keys)
			rangeForwards(buf, index, leaf.keys.drop(index), leaf.values.drop(index), leaf.next, length)
		} else {
			val index = before(k, leaf.keys)
			rangeBackwards(buf, index, leaf.keys.take(index).reverse, leaf.values.take(index).reverse, leaf.prev, -length)
		}
		buf.toSeq
	}

	private def after(key: K, keys: Seq[K]): Int = keys.lastIndexWhere(factory.ordering.gt(key, _)) + 1
	private def before(key: K, keys: Seq[K]): Int = keys.lastIndexWhere(factory.ordering.ge(key, _)) + 1

	private def searchByKey(k: K): LeafNode[K, V] = pathTo(k, root, Nil, after).head match { case l: LeafNode[K, V] => l; case _ => null }

	private def pathTo(k: K): List[Node[K]] = pathTo(k, root, Nil, before)
	private def pathTo(k: K, n: Node[K], path: List[Node[K]], indexBound: (K, Seq[K]) => Int): List[Node[K]] = {
		n match {
			case l: LeafNode[K, V] => l :: path
			case c: IndexNode[K] => pathTo(k, c.childAt(indexBound(k, c.keys)), c :: path, indexBound)
		}
	}

	private def searchByRank(rank: Position): Cursor = {
		@tailrec def searchByRank(n: Node[K], remainder: Position): Cursor = n match {
			case l: LeafNode[K, V] => {
				val index = remainder.intValue
				if (index < 0 || index >= l.count) null
				else Cursor(l.keyAt(index), l.childAt(index), l, index)
			}
			case c: IndexNode[K] => {
				val next = c.partialRanks.lastIndexWhere(rank >= _) + 1
				if (next >= c.partialRanks.size) null
				else searchByRank(c.childAt(next), remainder - (if (next == 0) 0: Position else c.partialRanks(next - 1)))
			}
		}
		searchByRank(root, rank)
	}

}