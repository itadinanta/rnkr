package net.itadinanta.rnkr.core.tree

import net.itadinanta.rnkr.core.tree._
import org.scalatest.Matchers

class RankTest extends TreeBaseTest with Matchers {
	test("A tree with one entry should have one rank 0") {
		val tree = createTestTree()
		tree.append(1, "Item")
		tree.size should be(1)
		tree.rank(1) should be(0)
	}

	test("A tree with few entries should have low ranks") {
		val tree = createTestTree()
		tree.append(1, "Item1")
		tree.append(2, "Item2")
		tree.append(3, "Item3")

		tree.size should be(3)
		tree.rank(1) should be(0)
		tree.rank(2) should be(1)
		tree.rank(3) should be(2)

		tree.page(1, 1) should be(Seq(Row(2, "Item2", 1)))
	}

	test("A tree with more entries should have higher ranks") {
		val tree = createTestTree((1, "Item1"), (2, "Item2"), (3, "Item3"), (4, "Item4"), (5, "Item5"), (6, "Item6"))
		debug(tree)
		tree.size should be(6)

		tree.rank(-1) should be(0)
		tree.rank(0) should be(0)
		tree.rank(1) should be(0)
		tree.rank(2) should be(1)
		tree.rank(3) should be(2)
		tree.rank(4) should be(3)
		tree.rank(5) should be(4)
		tree.rank(6) should be(5)
		tree.rank(7) should be(6)

		tree.page(-1, 1) should be(Seq())
		tree.page(0, 1) should be(Seq(Row(1, "Item1", 0)))
		tree.page(1, 1) should be(Seq(Row(2, "Item2", 1)))
		tree.page(2, 1) should be(Seq(Row(3, "Item3", 2)))
		tree.page(3, 1) should be(Seq(Row(4, "Item4", 3)))
		tree.page(4, 1) should be(Seq(Row(5, "Item5", 4)))
		tree.page(5, 1) should be(Seq(Row(6, "Item6", 5)))
		tree.page(6, 1) should be(Seq())

		tree.consistent should be(true)
	}

	test("Tree with ranges") {
		val tree = createTestTree((1, "Item1"), (2, "Item2"), (3, "Item3"), (5, "Item5"), (6, "Item6"), (7, "Item7"))

		tree.range(5, 2) shouldBe Seq(Row(5, "Item5", 3), Row(6, "Item6", 4))
		tree.range(3, 2) shouldBe Seq(Row(3, "Item3", 2), Row(5, "Item5", 3))
		tree.range(3, -2) shouldBe Seq(Row(3, "Item3", 2), Row(2, "Item2", 1))
		tree.range(4, 2) shouldBe Seq(Row(5, "Item5", 3), Row(6, "Item6", 4))
		tree.range(4, -2) shouldBe Seq(Row(3, "Item3", 2), Row(2, "Item2", 1))
	}

	test("Tree with ranks") {
		val tree = createTestTree((1, "Item1"), (2, "Item2"), (3, "Item3"), (5, "Item5"), (6, "Item6"), (7, "Item7"))

		tree.rank(0) should be(0)
		tree.rank(1) should be(0)
		tree.rank(5) should be(3)
		tree.rank(4) should be(3)
		tree.rank(8) should be(6)
		tree.rank(9) should be(6)
	}

	test("After 100 insertions with String keys should contain 100 ranks") {
		val tree = RankedTreeMap[String, String](StringAscending, 4)
		for (i <- 1 to 100) tree.append("Key%03d".format(i), "Item" + i)
		debug(s"Tree with Strings: ${tree}")
		tree.size should be(100)
		tree.factory.fanout should be(4)
		tree.level should be(4)
		tree.consistent should be(true)
		tree.get("Key007") should be(Some(Row("Key007", "Item7", 6)))
		for (i <- 1 to 100) { tree.get("Key%03d".format(i)) should be(Some(Row("Key%03d".format(i), "Item" + i, i - 1))) }
		for (i <- 1 to 100) { tree.rank("Key%03d".format(i)) should be(i - 1) }
	}

}