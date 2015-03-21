package net.itadinanta.rnkr.tree

import net.itadinanta.rnkr.node._
import org.scalatest.FlatSpec
import org.scalatest.ShouldMatchers
import scala.util.Random
import scala.collection.mutable
import org.slf4j.LoggerFactory
import org.fest.assertions.Assertions.assertThat

class DeleteTest extends TreeBaseTest {

	test("After 3 insertions and 1 deletions should contain 2 entries in the root") {
		Some(createTestTree((1, "Item"), (2, "Item"), (3, "Item"))) foreach { t =>
			debug(t)
			t.remove(1)
			debug(t)
			assertThat(t.size) isEqualTo 2
			assertThat(t.keys) isEqualTo Seq(2, 3)
		}

		Some(createTestTree((1, "Item"), (2, "Item"), (3, "Item"))) foreach { t =>
			debug(t)
			t.remove(2)
			debug(t)
			assertThat(t.size) isEqualTo 2
			assertThat(t.keys) isEqualTo Seq(1, 3)
		}

		Some(createTestTree((1, "Item"), (2, "Item"), (3, "Item"))) foreach { t =>
			debug(t)
			t.remove(3)
			debug(t)
			assertThat(t.size) isEqualTo 2
			assertThat(t.keys) isEqualTo Seq(1, 2)
		}
	}

	test("After 5 insertions and 1 deletion should contain 1 entry") {
		val tree = createTestTree(
			(1, "Item"),
			(2, "Item"),
			(3, "Item"),
			(4, "Item"),
			(5, "Item"))
		assertThat(tree.size) isEqualTo 5
		debug(tree)

		tree.remove(3)
		debug(tree)
		assertThat(tree.size) isEqualTo 4
		assertThat(tree.keys) isEqualTo Seq(1, 2, 4, 5)
		assertThat(tree.keysReverse) isEqualTo Seq(5, 4, 2, 1)
		assertThat(tree.consistent) isEqualTo true

		tree.remove(2)
		debug(tree)
		assertThat(tree.size) isEqualTo 3
		assertThat(tree.keys) isEqualTo Seq(1, 4, 5)
		assertThat(tree.keysReverse) isEqualTo Seq(5, 4, 1)
		assertThat(tree.root) isEqualTo tree.head
		assertThat(tree.consistent) isEqualTo true

		tree.remove(1)
		debug(tree)
		assertThat(tree.size) isEqualTo 2
		assertThat(tree.keys) isEqualTo Seq(4, 5)
		assertThat(tree.keysReverse) isEqualTo Seq(5, 4)
		assertThat(tree.root) isEqualTo tree.head
		assertThat(tree.consistent) isEqualTo true

	}

	test("After 100 insertions and 100 deletions should be empty") {
		val tree = createTestTree()
		val n = 100
		1 to n foreach { i =>
			tree.put(i, "Item" + i)
			assertThat(tree.keys()) isEqualTo (1 to i)
			assertThat(tree.consistent) isEqualTo true
		}
		debug(tree)
		1 to n foreach { i =>
			debug(s"Deleting item ${i} from ${tree}")
			tree.remove(i);
			debug(s"Deleted item {} from {}", i, tree)
			assertThat(tree.consistent) isEqualTo true
			assertThat(tree.keys()) isEqualTo ((i + 1) to n)
		}
		assertThat(tree.level) isEqualTo 1
		assertThat(tree.size) isEqualTo 0
		assertThat(tree.indexCount) isEqualTo 0
	}

	test("After 100 insertions and 100 deletions in reverse should be empty") {
		val tree = createTestTree()
		val n = 100
		1 to n foreach { i =>
			tree.put(i, "Item" + i)
			assertThat(tree.keys()) isEqualTo (1 to i)
		}
		debug(tree)
		n to 1 by -1 foreach { i =>
			tree.remove(i);
			debug(tree)
			assertThat(tree.keys()) isEqualTo (1 to (i - 1))
		}
		assertThat(tree.level) isEqualTo 1
		assertThat(tree.indexCount) isEqualTo 0
		assertThat(tree.size) isEqualTo 0
	}

	test("After 100 random insertions and 100 random deletions should be empty") {
		val tree = createTestTree()
		val ordered = new mutable.TreeSet[Int]
		val n = 100
		Random.setSeed(1234L)
		Random.shuffle(1 to n map { i => i }) foreach { i =>
			tree.put(i, "Item" + i)
			ordered += i
			assertThat(tree.keys()) isEqualTo ordered.toList
		}
		Random.shuffle(1 to n map { i => i }) foreach { i =>
			debug(s"Removing ${i} from ${tree}")
			tree.remove(i)
			debug(s"Removed ${i} from ${tree}")
			assertThat(tree.consistent) isEqualTo true
			ordered -= i
			assertThat(tree.keys()) isEqualTo ordered.toList
		}
		debug(s"Result ${tree}")
		assertThat(tree.size) isEqualTo 0
		assertThat(tree.indexCount) isEqualTo 0
		assertThat(tree.consistent) isEqualTo true
	}

	test("Deleting an item from the leaf should propagate to the root if necessary") {
		val tree = createTestTree()
		val ordered = new mutable.TreeSet[Int]
		val n = 13
		1 to n foreach { i =>
			tree.put(i, "Item" + i)
			ordered += i
			assertThat(tree.keys()) isEqualTo ordered.toList
		}
		assertThat(tree.leafCount) isEqualTo 6

		Seq(11, 7, 5, 9, 1, 6, 12) foreach { i =>
			debug(s"Removing ${i} from {tree}")
			tree.remove(i)
			debug(s"Removed ${i} from ${tree}")
			ordered -= i
			assertThat(tree.keys()) isEqualTo ordered.toList
			assertThat(tree.consistent) isEqualTo true
		}
		debug(s"Result ${tree}")
		assertThat(tree.size) isEqualTo ordered.size
		assertThat(tree.leafCount) isEqualTo 2
		assertThat(tree.indexCount) isEqualTo 1
	}

	test("After 1000 random insertions and 10000 random deletions should be empty") {
		val tree = createTreeWithFanout(32)
		val ordered = new mutable.TreeSet[Int]
		val n = 1000
		Random.setSeed(1234L)
		Random.shuffle(1 to n map { i => i }) foreach { i =>
			tree.put(i, "Item" + i)
			ordered += i
			assertThat(tree.keys()) isEqualTo ordered.toList
		}
		Random.shuffle(1 to n map { i => i }) foreach { i =>
			tree.remove(i)
			assertThat(tree.consistent) isEqualTo true
			ordered -= i
			assertThat(tree.keys()) isEqualTo ordered.toList
		}
		debug("Result ${tree}")
		assertThat(tree.size) isEqualTo 0
		assertThat(tree.indexCount) isEqualTo 0
		assertThat(tree.consistent) isEqualTo true
	}

}