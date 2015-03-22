package net.itadinanta.rnkr.tree

import net.itadinanta.rnkr.node._
import org.scalatest.FlatSpec
import org.scalatest.ShouldMatchers
import scala.util.Random
import scala.collection.mutable
import org.slf4j.LoggerFactory

class DeleteTest extends TreeBaseTest {

	test("After 3 insertions and 1 deletions should contain 2 entries in the root") {
		Some(createTestTree((1, "Item"), (2, "Item"), (3, "Item"))) foreach { t =>
			debug(t)
			t.remove(1)
			debug(t)
			t.size should be(2)
			t.keys should be(Seq(2, 3))
		}

		Some(createTestTree((1, "Item"), (2, "Item"), (3, "Item"))) foreach { t =>
			debug(t)
			t.remove(2)
			debug(t)
			t.size should be(2)
			t.keys should be(Seq(1, 3))
		}

		Some(createTestTree((1, "Item"), (2, "Item"), (3, "Item"))) foreach { t =>
			debug(t)
			t.remove(3)
			debug(t)
			t.size should be(2)
			t.keys should be(Seq(1, 2))
		}
	}

	test("After 5 insertions and 1 deletion should contain 1 entry") {
		val tree = createTestTree(
			(1, "Item"),
			(2, "Item"),
			(3, "Item"),
			(4, "Item"),
			(5, "Item"))
		tree.size should be(5)
		debug(tree)

		tree.remove(3)
		debug(tree)
		tree.size should be(4)
		tree.keys should be(Seq(1, 2, 4, 5))
		tree.keysReverse should be(Seq(5, 4, 2, 1))
		tree.consistent should be(true)

		tree.remove(2)
		debug(tree)
		tree.size should be(3)
		tree.keys should be(Seq(1, 4, 5))
		tree.keysReverse should be(Seq(5, 4, 1))
		tree.root should be(tree.head)
		tree.consistent should be(true)

		tree.remove(1)
		debug(tree)
		tree.size should be(2)
		tree.keys should be(Seq(4, 5))
		tree.keysReverse should be(Seq(5, 4))
		tree.root should be(tree.head)
		tree.consistent should be(true)

	}

	test("After 100 insertions and 100 deletions should be empty") {
		val tree = createTestTree()
		val n = 100
		1 to n foreach { i =>
			tree.put(i, "Item" + i)
			tree.keys() should be((1 to i))
			tree.consistent should be(true)
		}
		debug(tree)
		1 to n foreach { i =>
			debug(s"Deleting item ${i} from ${tree}")
			tree.remove(i);
			debug(s"Deleted item {} from {}", i, tree)
			tree.consistent should be(true)
			tree.keys() should be(((i + 1) to n))
		}
		tree.level should be(1)
		tree.size should be(0)
		tree.indexCount should be(0)
	}

	test("After 100 insertions and 100 deletions in reverse should be empty") {
		val tree = createTestTree()
		val n = 100
		1 to n foreach { i =>
			tree.put(i, "Item" + i)
			tree.keys() should be((1 to i))
		}
		debug(tree)
		n to 1 by -1 foreach { i =>
			tree.remove(i);
			debug(tree)
			tree.keys() should be((1 to (i - 1)))
		}
		tree.level should be(1)
		tree.indexCount should be(0)
		tree.size should be(0)
	}

	test("After 100 random insertions and 100 random deletions should be empty") {
		val tree = createTestTree()
		val ordered = new mutable.TreeSet[Int]
		val n = 100
		Random.setSeed(1234L)
		Random.shuffle(1 to n map { i => i }) foreach { i =>
			tree.put(i, "Item" + i)
			ordered += i
			tree.keys() should be(ordered.toList)
		}
		Random.shuffle(1 to n map { i => i }) foreach { i =>
			debug(s"Removing ${i} from ${tree}")
			tree.remove(i)
			debug(s"Removed ${i} from ${tree}")
			tree.consistent should be(true)
			ordered -= i
			tree.keys() should be(ordered.toList)
		}
		debug(s"Result ${tree}")
		tree.size should be(0)
		tree.indexCount should be(0)
		tree.consistent should be(true)
	}

	test("Deleting an item from the leaf should propagate to the root if necessary") {
		val tree = createTestTree()
		val ordered = new mutable.TreeSet[Int]
		val n = 13
		1 to n foreach { i =>
			tree.put(i, "Item" + i)
			ordered += i
			tree.keys() should be(ordered.toList)
		}
		tree.leafCount should be(6)

		Seq(11, 7, 5, 9, 1, 6, 12) foreach { i =>
			debug(s"Removing ${i} from {tree}")
			tree.remove(i)
			debug(s"Removed ${i} from ${tree}")
			ordered -= i
			tree.keys() should be(ordered.toList)
			tree.consistent should be(true)
		}
		debug(s"Result ${tree}")
		tree.size should be(ordered.size)
		tree.leafCount should be(2)
		tree.indexCount should be(1)
	}

	test("After 1000 random insertions and 10000 random deletions should be empty") {
		val tree = createTreeWithFanout(32)
		val ordered = new mutable.TreeSet[Int]
		val n = 1000
		Random.setSeed(1234L)
		Random.shuffle(1 to n map { i => i }) foreach { i =>
			tree.put(i, "Item" + i)
			ordered += i
			tree.keys() should be(ordered.toList)
		}
		Random.shuffle(1 to n map { i => i }) foreach { i =>
			tree.remove(i)
			tree.consistent should be(true)
			ordered -= i
			tree.keys() should be(ordered.toList)
		}
		debug("Result ${tree}")
		tree.size should be(0)
		tree.indexCount should be(0)
		tree.consistent should be(true)
	}

}