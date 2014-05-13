package net.itadinanta.rnkr.node

import org.scalatest.FlatSpec
import org.scalatest.ShouldMatchers
import net.itadinanta.rnkr.tree.SeqNodeFactory

class NodeTest extends FlatSpec with ShouldMatchers {
	val builder = new SeqNodeFactory[Int, String]

	"IntAscending" should "sort ascending" in {
		IntAscending.lt(0, 0) should be(false)
		IntAscending.lt(0, 1) should be(true)
		IntAscending.lt(1, 0) should be(false)

		IntAscending.gt(0, 0) should be(false)
		IntAscending.gt(0, 1) should be(false)
		IntAscending.gt(1, 0) should be(true)

		IntAscending.gt(3, 2) should be(true)

		Seq(1, 2).lastIndexWhere(item => IntAscending.gt(0, item)) should be(-1)
		Seq(1, 2).lastIndexWhere(item => IntAscending.gt(1, item)) should be(-1)
		Seq(1, 2).lastIndexWhere(item => IntAscending.gt(2, item)) should be(0)
		Seq(1, 2).lastIndexWhere(item => IntAscending.gt(3, item)) should be(1)
	}

	"An empty node" should "contain no entries" in {
		builder.data.newNode().keys.size should be === 0
	}

	"A node after insertion" should "contain 1 entry" in {
		val newNode = builder.data.newNode(1, "Value")
		newNode.keys.length should be === 1
	}

	"A node after value insertion" should "contain 1 entry" in {
		val newNode = builder.data.newNode(1, "Value")
		val addition = builder.data.insert(newNode, 2, "AnotherValue")
		addition.node.keys.length should be === 2
		addition.node.keys should be === Seq(1, 2)
	}

	"A node after value insertions" should "contain entries in order" in {
		val one = builder.data.newNode(1, "One")
		val two = builder.data.insert(one, 2, "Two")
		val three = builder.data.insert(two.node, 3, "Three")
		three.node.keys.length should be === 3
		three.node.keys should be === Seq(1, 2, 3)
		three.node.values should be === Seq("One", "Two", "Three")
	}

	"A node after value insertions not in order" should "contain entries in order" in {
		val one = builder.data.newNode(1, "One")
		val two = builder.data.insert(one, 3, "Three")
		val three = builder.data.insert(two.node, 2, "Two")
		three.node.keys.length should be === 3
		three.node.keys should be === Seq(1, 2, 3)
		three.node.values should be === Seq("One", "Two", "Three")
	}

}