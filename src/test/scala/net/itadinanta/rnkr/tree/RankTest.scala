package net.itadinanta.rnkr.tree

import net.itadinanta.rnkr.node._
import scala.util.Random
import scala.collection.mutable
import org.slf4j.LoggerFactory
import org.fest.assertions.Assertions.assertThat
import org.junit.Test

class RankTest extends TreeBaseTest {
	test("A tree with one entry should have one rank 0") {
		val tree = createTestTree()
		tree.append(1, "Item")
		assertThat(tree.size) isEqualTo 1
		assertThat(tree.rank(1)) isEqualTo 0
	}

	test("A tree with few entries should have low ranks") {
		val tree = createTestTree()
		tree.append(1, "Item1")
		tree.append(2, "Item2")
		tree.append(3, "Item3")

		assertThat(tree.size) isEqualTo 3
		assertThat(tree.rank(1)) isEqualTo 0
		assertThat(tree.rank(2)) isEqualTo 1
		assertThat(tree.rank(3)) isEqualTo 2

		assertThat(tree.page(1, 1)) isEqualTo Seq(Row(2, "Item2", 1))
	}

	test("A tree with more entries should have higher ranks") {
		val tree = createTestTree((1, "Item1"), (2, "Item2"), (3, "Item3"), (4, "Item4"), (5, "Item5"), (6, "Item6"))
		log.debug("{}", tree)
		assertThat(tree.size) isEqualTo 6

		assertThat(tree.rank(-1)) isEqualTo -1
		assertThat(tree.rank(0)) isEqualTo -1
		assertThat(tree.rank(1)) isEqualTo 0
		assertThat(tree.rank(2)) isEqualTo 1
		assertThat(tree.rank(3)) isEqualTo 2
		assertThat(tree.rank(4)) isEqualTo 3
		assertThat(tree.rank(5)) isEqualTo 4
		assertThat(tree.rank(6)) isEqualTo 5
		assertThat(tree.rank(7)) isEqualTo 6

		assertThat(tree.page(-1, 1)) isEqualTo Seq()
		assertThat(tree.page(0, 1)) isEqualTo Seq(Row(1, "Item1", 0))
		assertThat(tree.page(1, 1)) isEqualTo Seq(Row(2, "Item2", 1))
		assertThat(tree.page(2, 1)) isEqualTo Seq(Row(3, "Item3", 2))
		assertThat(tree.page(3, 1)) isEqualTo Seq(Row(4, "Item4", 3))
		assertThat(tree.page(4, 1)) isEqualTo Seq(Row(5, "Item5", 4))
		assertThat(tree.page(5, 1)) isEqualTo Seq(Row(6, "Item6", 5))
		assertThat(tree.page(6, 1)) isEqualTo Seq()

		assertThat(tree.consistent) isEqualTo true
	}

}