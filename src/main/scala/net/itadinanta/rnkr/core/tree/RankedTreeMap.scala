package net.itadinanta.rnkr.core.tree

import scala.annotation.tailrec
import scala.volatile
import scala.collection.mutable.ListBuffer
import Rank.Position
import grizzled.slf4j.Logging

object RankedTreeMap {
	implicit val defaultFactory = new SeqNodeFactory[Int, String](IntAscending, 20)
	def create() = new SeqTree[Int, String](defaultFactory)

	def withIntKeys[T](ordering: Ordering[Int] = IntAscending, fanout: Int = 50) =
		new SeqTree[Int, T](new SeqNodeFactory[Int, T](ordering, fanout))

	def withStringKeys[T](ordering: Ordering[String] = StringAscending, fanout: Int = 50) =
		new SeqTree[String, T](new SeqNodeFactory[String, T](ordering, fanout))

	def withStringValues[K](ordering: Ordering[K], fanout: Int = 50) =
		new SeqTree[K, String](new SeqNodeFactory[K, String](ordering, fanout))

	def withLongKeys[T](ordering: Ordering[Long] = LongAscending, fanout: Int = 50) =
		new SeqTree[Long, T](new SeqNodeFactory[Long, T](ordering, fanout))

	def apply[K, V](ordering: Ordering[K], fanout: Int = 50) =
		new SeqTree[K, V](new SeqNodeFactory[K, V](ordering, fanout))
}

trait RankedTreeMap[K, V] {
	def size: Int
	def isEmpty: Boolean
	def version: Long
	def get(k: K): Option[Row[K, V]]
	def remove(k: K): Option[Row[K, V]]
	def put(k: K, value: V): Row[K, V]
	def append(k: K, value: V): Row[K, V]
	def keys(): Seq[K]
	def values(): Seq[V]
	def entries(): Seq[(K,V)]
	def keysReverse(): Seq[K]
	def rank(k: K): Position
	def range(k: K, length: Int): Seq[Row[K, V]]
	def page(start: Position, length: Int): Seq[Row[K, V]]
	def clear(): Int
}

object SeqTree extends Logging {
	val LOG = logger.logger
}

class SeqTree[K, V](val factory: NodeFactory[K, V]) extends RankedTreeMap[K, V] {
	import SeqTree.LOG
	case class Cursor(val key: K, val value: V, val node: LeafNode[K, V], val index: Position, val offset: Position)
	case class CursorOption(val key: Option[K], val value: Option[V], val node: LeafNode[K, V], val index: Position, val offset: Position)

	private[rnkr] var head: LeafNode[K, V] = _
	private[rnkr] var root: Node[K] = _
	private[rnkr] var tail: LeafNode[K, V] = _
	private[rnkr] var valueCount: Int = _
	private[rnkr] var leafCount: Int = _
	private[rnkr] var indexCount: Int = _
	private[rnkr] var level: Int = _
	@volatile private[rnkr] var _version: Long = _

	clear()

	override def clear() = {
		val oldSize = size

		head = factory.data.newNode()
		root = head
		tail = head
		valueCount = 0
		leafCount = 1
		indexCount = 0
		level = 1
		_version = 0

		oldSize
	}

	override def version = _version
	override def size = valueCount
	override def isEmpty = valueCount == 0
	override def rank(k: K): Position =
		searchByKey(k) match {
			case CursorOption(None, None, node, index, offset) => before(k, node.keys) + offset
			case CursorOption(_, _, _, index, offset) => index + offset
		}

	private def newVersion() = { _version += 1; _version }
	private def checkVersion[R](cv: Long)(f: => R): R = {
		if (_version != cv) throw new RuntimeException(s"Expected version ${cv} detected version ${_version}")
		f
	}

	override def append(k: K, v: V) = checkVersion(newVersion) {
		val inserted = insertAtEnd(k, v)
		Row(k, inserted.value, size)
	}

	override def put(k: K, v: V) = checkVersion(newVersion) {
		val inserted = insert(k, v)
		Row(k, v, inserted.offset + inserted.index)
	}

	override def get(k: K) = {
		@tailrec def row(n: Node[K], p: Position): Option[Row[K, V]] = {
			n match {
				case l: LeafNode[K, V] => {
					val i = l.indexOfKey(k)
					if (i < 0) None
					else Some(Row(k, l.childAt(i), p + i))
				}
				case c: IndexNode[K] => {
					val index = before(k, c.keys)
					row(c.childAt(index), if (index == 0) p else p + c.partialRanks(index - 1))
				}
			}
		}
		row(root, 0)
	}

	private[this] def traverse[I](node: LeafNode[K, V], nav: (LeafNode[K, V]) => LeafNode[K, V])(app: (LeafNode[K, V]) => Seq[I]) = {
		val buf = new ListBuffer[I]
		@tailrec def traverse(node: LeafNode[K, V]): Unit = if (node != null) {
			buf ++= app(node)
			traverse(nav(node))
		}
		traverse(node)
		buf.toList
	}

	private[this] def forwards[I](app: (LeafNode[K, V]) => Seq[I]) = traverse(head, { _.next })(app)
	private[this] def backwards[I](app: (LeafNode[K, V]) => Seq[I]) = traverse(tail, { _.prev })(app)

	override def keys() = forwards { _.keys }
	override def entries() = forwards { node => node.keys zip node.values }
	override def values() = forwards { _.values }
	override def keysReverse() = backwards { _.keys.reverse }

	override def remove(k: K) = checkVersion(newVersion) {
		delete(k) match {
			case Cursor(key, child, _, index, offset) => Some(Row(key, child, index))
			case _ => None
		}
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
			val result = (indexKeys.size == (indexValues.size - 1)) &&
				(index.isEmpty ||
					(factory.index.ordering.lt(min(indexValues.head), indexKeys.head) &&
						indexKeys.zip(indexValues.tail.map(min)).foldLeft(true) { (a, b) => a && (b._1 == b._2) } &&
						indexPartialRanks.zip(indexValues.map(count).scanLeft(0: Position)(_ + _).tail).foldLeft(true) { (a, b) => a && (b._1 == b._2) } &&
						indexValues.foldLeft(true) { (a, b) => (a && consistent(b)) }))
			if (!result) {
				LOG.warn("inconsistent {}", index.topLevel)
			}
			result
		}
	}

	def consistent: Boolean = consistent(root)

	private def insertAtEnd(k: K, v: V): Cursor = {
		case class AppendedResult(a: Node[K], key: K, b: Option[Node[K]], cursor: Cursor)

		def appendRecursively(node: Node[K], k: K, v: V): AppendedResult = node match {
			case leaf: LeafNode[K, V] => {
				if (leaf.isEmpty || factory.ordering.gt(k, leaf.keys.last)) {
					val appended = factory.data.append(leaf, k, v)
					valueCount += 1
					val cursor = Cursor(k, v, leaf, appended.index, 0) // TBC
					if (appended.split) {
						appended.b.next = appended.a.next
						appended.a.next = appended.b
						tail = appended.b
						tail.prev = appended.a
						leafCount += 1
						AppendedResult(appended.a, appended.key, Some(appended.b), cursor)
					} else
						AppendedResult(appended.a, appended.key, None, cursor)
				} else throw new IllegalArgumentException("Key " + k + " out of order")
			}
			case index: IndexNode[K] => {
				if (factory.ordering.gt(k, index.keys.last)) {
					appendRecursively(index.values.last, k, v) match {
						case AppendedResult(a, newKey, None, cursor) =>
							AppendedResult(factory.index.grow(index, index.values.size - 1, +1), newKey, None, cursor)
						case AppendedResult(a, newKey, Some(b), cursor) => {
							val inserted = factory.index.append(index, newKey, b, index.partialRanks.last + 1)
							if (inserted.split) {
								indexCount += 1
								factory.index.offload(inserted.b, inserted.b.values.size - 2, -b.count + 1)
								AppendedResult(inserted.a, inserted.key, Some(inserted.b), cursor)
							} else {
								factory.index.offload(inserted.a, inserted.a.values.size - 2, -b.count + 1)
								AppendedResult(inserted.a, inserted.key, None, cursor)
							}
						}
					}
				} else throw new IllegalArgumentException("Key " + k + " out of order")
			}
		}

		val appended = appendRecursively(root, k, v)
		root = appended match {
			case AppendedResult(a, key, Some(b), cursor) => newRoot(key, a, b)
			case _ => root
		}

		appended.cursor
	}

	private def insert(k: K, v: V): Cursor = {
		sealed trait ChangedResult { val a: Node[K]; val key: K; val cursor: Cursor }
		case class InsertedResult(override val a: Node[K], override val key: K, val b: Option[Node[K]], val taken: Position, override val cursor: Cursor) extends ChangedResult
		case class UpdatedResult(override val a: Node[K], override val key: K, override val cursor: Cursor) extends ChangedResult

		def insertRecursively(node: Node[K], k: K, v: V, offset: Position): ChangedResult = node match {
			case leaf: LeafNode[K, V] => {
				val i = node.indexOfKey(k)
				if (i >= 0)
					UpdatedResult(leaf, k, Cursor(k, v, leaf, factory.data.update(leaf, k, v).index, offset))
				else {
					val inserted = factory.data.insert(leaf, k, v, 1)
					valueCount += 1
					val cursor = Cursor(k, v, leaf, inserted.index, offset)
					if (inserted.split) {
						inserted.b.next = leaf.next
						inserted.a.next = inserted.b
						inserted.b.prev = inserted.a
						if (inserted.b.next != null) inserted.b.next.prev = inserted.b
						head = if (leaf eq head) inserted.a else head
						tail = if (leaf eq tail) inserted.b else tail
						leafCount += 1
						InsertedResult(inserted.a, inserted.key, Some(inserted.b), inserted.b.count, cursor)
					} else
						InsertedResult(inserted.a, inserted.key, None, -1, cursor)
				}
			}
			case index: IndexNode[K] => {
				val i = index.keys.lastIndexWhere(factory.ordering.ge(k, _)) + 1
				insertRecursively(index.childAt(i), k, v, offset + index.partialRankAt(i - 1)) match {
					case u: UpdatedResult => { u }
					case InsertedResult(a, newKey, None, taken, cursor) => {
						InsertedResult(factory.index.grow(index, i, -taken), newKey, None, taken, cursor)
					}
					case InsertedResult(a, newKey, Some(b), taken, cursor) => {
						val inserted = factory.index.insert(index, newKey, b, 1 - taken, taken)
						if (inserted.split) {
							indexCount += 1
							InsertedResult(inserted.a, inserted.key, Some(inserted.b), inserted.b.count, cursor)
						} else {
							InsertedResult(index, inserted.key, None, -1, cursor)
						}
					}
				}
			}
		}

		val inserted = insertRecursively(root, k, v, 0)
		root = inserted match {
			case InsertedResult(a, key, Some(b), _, cursor) => newRoot(key, a, b)
			case _ => root
		}

		inserted.cursor
	}

	private def newRoot(key: K, a: Node[K], b: Node[K]) = {
		level += 1
		indexCount += 1
		factory.index.newNode(Seq(key), Seq(a, b), Seq(a.count, a.count + b.count))
	}

	private def toInt(in: Int): Object = new java.lang.Integer(in.intValue())
	private def toLong(in: Long): Object = new java.lang.Long(in.longValue())

	private def delete(k: K): Cursor = {
		case class DeletedResult(pos: Int,
			ak: K,
			oldAk: K,
			a: Node[K],
			bk: Option[K],
			oldBk: Option[K],
			takenA: Position,
			takenB: Position,
			cursor: Cursor)

		def deleteRecursively(k: K, pos: Int, left: Option[Node[K]], node: Node[K], right: Option[Node[K]]): DeletedResult = {
			// choose siblings
			lazy val (firstSibling, av, bv) = (left, right) match {
				case (None, Some(rv)) => (pos, node, rv)
				case (Some(lv), None) => (pos - 1, lv, node)
				case (Some(lv), Some(rv)) => if (lv.keys.size > rv.keys.size) (pos - 1, lv, node) else (pos, node, rv)
				case (None, None) => (pos, node, node)
			}

			node match {
				case leaf: LeafNode[K, V] => {
					val leafK = leaf.keys.head
					val deleted = factory.data.delete(leaf, k)
					valueCount -= 1
					val cursor = Cursor(k, deleted.child, leaf, deleted.index, 0)
					// LOG.debug("Deleted index {} from {}", deleted.index, deleted.a);
					if (factory.data.balanced(leaf) || (av eq bv))
						DeletedResult(pos, if (leaf.isEmpty) k else leaf.keyAt(0), leafK, leaf, None, None, -1, 0, cursor)
					else (av, bv) match {
						case (a: LeafNode[K, V], b: LeafNode[K, V]) => {
							// LOG.debug("Leaf needs rebalancing after deletion {} {}", Array[Object](a, b): _*);
							val oldAk = if (a eq leaf) leafK else a.keys.head
							val oldBk = if (b eq leaf) leafK else b.keys.head
							val initialACount = a.count + (if (a eq leaf) 1 else 0)
							val initialBCount = b.count + (if (b eq leaf) 1 else 0)
							// LOG.debug("Rebalancing {}[{}] {}[{}]", Array[Object](a, toLong(initialACount), b, toLong(initialBCount)))
							val balanced = factory.data.redistribute(a, Seq(), b)

							if (balanced.b.isEmpty) {
								// special case for LeafNode
								balanced.a.next = balanced.b.next
								if (balanced.a.next != null) balanced.a.next.prev = balanced.a
								if (balanced.b eq tail) tail = balanced.a
								leafCount -= 1
							}
							// LOG.debug("Rebalanced leaf {} {}", Array[Object](balanced.a, balanced.b): _*);
							DeletedResult(firstSibling, balanced.a.keyAt(0), oldAk, balanced.a, balanced.b.keyOption(0), Some(oldBk),
								balanced.a.count - initialACount, balanced.b.count - initialBCount,
								cursor)
						}
					}
				}
				case index: IndexNode[K] => {
					val candidateChild = index.keys.lastIndexWhere(factory.ordering.ge(k, _)) + 1
					deleteRecursively(k, candidateChild, index.childOption(candidateChild - 1), index.childAt(candidateChild), index.childOption(candidateChild + 1)) match {
						case DeletedResult(child, ak, oldAk, a, None, Some(oldPivot), takenA, takenB, cursor) => {
							// LOG.debug("Removed empty node {} {} {}", Array[Object](toLong(takenA), toLong(takenB), index))
							factory.index.grow(index, child, takenA)
							if (candidateChild > 0 && index.keyAt(candidateChild - 1) == oldAk) factory.index.renameKeyAt(index, candidateChild - 1, ak)
							val deleted = factory.index.deleteAt(index, child)
							if (factory.index.balanced(index) || (av eq bv)) {
								DeletedResult(pos, ak, oldAk, index, None, None, takenA, takenB, cursor)
							} else (av, bv) match {
								case (a: IndexNode[K], b: IndexNode[K]) => {
									val pivot = first(b)
									val oldBk = if (b eq index) oldPivot else b.keys.head
									val initialACount = a.count + (if (a eq index) 1 else 0)
									val initialBCount = b.count + (if (b eq index) 1 else 0)
									// LOG.debug("Rebalancing {}[{}] {}[{}]", Array[Object](a, toLong(initialACount), b, toLong(initialBCount)): _*)
									val balanced = factory.index.redistribute(a, Seq(pivot), b)
									// LOG.debug("Rebalanced {} {}", Array[Object](balanced.a, balanced.b): _*);
									if (balanced.b.isEmpty) {
										indexCount -= 1
										DeletedResult(firstSibling, ak, oldAk, balanced.a, None, Some(pivot),
											balanced.a.count - initialACount, -initialBCount,
											cursor)
									} else {
										DeletedResult(firstSibling, ak, oldAk, balanced.a, Some(balanced.key), Some(pivot),
											balanced.a.count - initialACount, balanced.b.count - initialBCount,
											cursor)
									}
								}
							}
						}

						case DeletedResult(child, ak, oldAk, a, Some(bk), oldBk, takenA, takenB, cursor) => {
							// LOG.debug("Rebalanced both nodes {} {} {}", Array[Object](toLong(takenA), toLong(takenB), index))
							if (takenA != 0)
								factory.index.grow(index, child, takenA)
							if (takenB != 0)
								factory.index.grow(index, child + 1, takenB)

							if (ak != oldAk && candidateChild > 0 && index.keyAt(candidateChild - 1) == oldAk) factory.index.renameKeyAt(index, candidateChild - 1, ak)
							oldBk foreach { k =>
								if (bk != k) {
									if (candidateChild > 0 && index.keyAt(candidateChild - 1) == k) factory.index.renameKeyAt(index, candidateChild - 1, bk)
									else if (index.keyAt(candidateChild) == k) factory.index.renameKeyAt(index, candidateChild, bk)
								}
							}
							DeletedResult(pos, ak, oldAk, index, None, None, takenA, takenB, cursor)
						}

						case DeletedResult(child, k, oldK, node, None, None, takenA, takenB, cursor) => {
							// LOG.debug("Propagating deletion [{}] {} {} {} to {}", Array[Object](toInt(child), toLong(takenA), toLong(takenB), index, toInt(candidateChild - 1)))
							factory.index.grow(index, child, takenA + takenB) // WTF?
							if (k != oldK && candidateChild > 0 && index.keyAt(candidateChild - 1) == oldK) factory.index.renameKeyAt(index, candidateChild - 1, k)
							DeletedResult(pos, k, oldK, index, None, None, takenA, takenB, cursor)
						}
					}
				}
			}
		}

		deleteRecursively(k, 0, None, root, None) match {
			case d: DeletedResult => {
				root = root match {
					case index: IndexNode[K] => if (index.isEmpty) {
						indexCount -= 1;
						level -= 1;
						index.childAt(0)
					} else root
					case _ => root
				}
				d.cursor
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
		val cursor = searchByKey(k)
		val leaf = cursor.node
		val buf = new ListBuffer[Row[K, V]]
		if (length >= 0) {
			val index = after(k, leaf.keys)
			rangeForwards(buf, index + cursor.offset, leaf.keys.drop(index), leaf.values.drop(index), leaf.next, length)
		} else {
			//val index = cursor.index.intValue // ?
			val index = before(k, leaf.keys)
			//val index = cursor.index.intValue
			rangeBackwards(buf, index + cursor.offset - 1, leaf.keys.take(index).reverse, leaf.values.take(index).reverse, leaf.prev, -length)
		}
		buf.toSeq
	}

	private def after(key: K, keys: Seq[K]): Int = keys.lastIndexWhere(factory.ordering.gt(key, _)) + 1
	private def before(key: K, keys: Seq[K]): Int = keys.lastIndexWhere(factory.ordering.ge(key, _)) + 1

	private def searchByKey(k: K): CursorOption = {
		@tailrec def searchByKey(n: Node[K], p: Position): CursorOption = {
			n match {
				case l: LeafNode[K, V] => {
					val i = l.indexOfKey(k)

					if (i >= 0) CursorOption(Some(l.keyAt(i)), Some(l.childAt(i)), l, i, p)
					else if (p == 0) CursorOption(None, None, l, 0, 0)
					else CursorOption(None, None, l, l.count, p)
				}
				case c: IndexNode[K] => {
					val index = before(k, c.keys)
					searchByKey(c.childAt(index), if (index == 0) p else p + c.partialRanks(index - 1))
				}
			}
		}
		searchByKey(root, 0)
	}

	private def searchByRank(rank: Position): Cursor = {
		@tailrec def searchByRank(n: Node[K], remainder: Position): Cursor = n match {
			case l: LeafNode[K, V] => {
				val index = remainder.intValue
				if (index < 0 || index >= l.count) null
				else Cursor(l.keyAt(index), l.childAt(index), l, index, rank - index)
			}
			case c: IndexNode[K] => {
				val next = c.partialRanks.lastIndexWhere(remainder >= _) + 1
				if (next >= c.partialRanks.size) null
				else searchByRank(c.childAt(next), remainder - (if (next == 0) 0: Position else c.partialRanks(next - 1)))
			}
		}
		searchByRank(root, rank)
	}

}