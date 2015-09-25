package net.itadinanta.rnkr.core.tree

import net.itadinanta.rnkr.core.tree._
import scala.util.Random
import scala.collection.mutable

class DeleteTest extends TreeBaseTest {

	test("After 3 insertions and 1 deletions should contain 2 entries in the root") {
		Some(createTestTree((1, "Item"), (2, "Item"), (3, "Item"))) foreach { t =>
			t.remove(1)
			t.size should be(2)
			t.keys should be(Seq(2, 3))
		}

		Some(createTestTree((1, "Item"), (2, "Item"), (3, "Item"))) foreach { t =>
			t.remove(2)
			t.size should be(2)
			t.keys should be(Seq(1, 3))
		}

		Some(createTestTree((1, "Item"), (2, "Item"), (3, "Item"))) foreach { t =>
			t.remove(3)
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

		tree.remove(3)
		tree.size should be(4)
		tree.keys should be(Seq(1, 2, 4, 5))
		tree.keysReverse should be(Seq(5, 4, 2, 1))
		tree.consistent should be(true)

		tree.remove(2)
		tree.size should be(3)
		tree.keys should be(Seq(1, 4, 5))
		tree.keysReverse should be(Seq(5, 4, 1))
		tree.root should be(tree.head)
		tree.consistent should be(true)

		tree.remove(1)
		tree.size should be(2)
		tree.keys should be(Seq(4, 5))
		tree.keysReverse should be(Seq(5, 4))
		tree.root should be(tree.head)
		tree.consistent should be(true)

	}

	test(s"After ${smallCount} insertions and ${smallCount} deletions should be empty") {
		val tree = createTestTree()
		val n = smallCount
		for (i <- 1 to n) {
			tree.put(i, "Item" + i)
			tree.keys() should be((1 to i))
			tree.consistent should be(true)
		}
		1 to n foreach { i =>
			tree.remove(i);
			tree.consistent should be(true)
			tree.keys() should be(((i + 1) to n))
		}
		tree.level should be(1)
		tree.size should be(0)
		tree.indexCount should be(0)
	}

	test(s"After ${smallCount} insertions and ${smallCount} deletions in reverse should be empty") {
		val tree = createTestTree()
		val n = smallCount
		for (i <- 1 to n) {
			tree.put(i, "Item" + i)
			tree.keys() should be((1 to i))
		}
		for (i <- n to 1 by -1) {
			tree.remove(i);
			tree.keys() should be((1 until i))
		}
		tree.level should be(1)
		tree.indexCount should be(0)
		tree.size should be(0)
	}

	test(s"After ${smallCount} random insertions and ${smallCount} random deletions should be empty") {
		val tree = createTestTree()
		val ordered = new mutable.TreeSet[Int]
		val n = smallCount
		val rnd = new Random
		rnd.setSeed(1234L)
		rnd.shuffle(1 to n) foreach { i =>
			tree.put(i, "Item" + i)
			ordered += i
			tree.keys() should be(ordered.toList)
		}
		rnd.shuffle(1 to n) foreach { i =>
			tree.remove(i)
			tree.consistent should be(true)
			ordered -= i
			tree.keys() should be(ordered.toList)
		}
		tree.size should be(0)
		tree.indexCount should be(0)
		tree.consistent should be(true)
	}

	test("Deleting an item from the leaf should propagate to the root if necessary") {
		val tree = createTestTree()
		val ordered = new mutable.TreeSet[Int]
		val n = 13
		for (i <- 1 to n) {
			tree.put(i, "Item" + i)
			ordered += i
			tree.keys() should be(ordered.toList)
		}
		tree.leafCount should be(6)

		for (i <- Seq(11, 7, 5, 9, 1, 6, 12)) {
			tree.remove(i)
			ordered -= i
			tree.keys() should be(ordered.toList)
			tree.consistent should be(true)
		}
		tree.size should be(ordered.size)
		tree.leafCount should be(2)
		tree.indexCount should be(1)
	}

	val someCount = 1000
	test(s"After ${someCount} random insertions and ${someCount} random deletions should be empty") {
		val tree = createTreeWithFanout(32)
		val ordered = new mutable.TreeSet[Int]
		val n = someCount
		val rnd = new Random
		rnd.setSeed(1234L)
		rnd.shuffle(1 to n) foreach { i =>
			tree.put(i, "Item" + i)
			ordered += i
			tree.keys() should be(ordered.toList)
		}
		rnd.shuffle(1 to n map { i => i }) foreach { i =>
			tree.remove(i)
			tree.consistent should be(true)
			ordered -= i
			tree.keys() should be(ordered.toList)
		}
		tree.size should be(0)
		tree.indexCount should be(0)
		tree.consistent should be(true)
	}

}