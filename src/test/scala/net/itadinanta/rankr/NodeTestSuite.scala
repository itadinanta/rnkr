package net.itadinanta.rankr;

import net.itadinanta.rnkr.node._
import org.scalatest.FlatSpec
import org.scalatest.ShouldMatchers

class NodeTestSuite extends FlatSpec with ShouldMatchers {
	val builder = new NodeBuilder[Int, String](IntAscending)
	"An empty node" should "contain no entries" in {
		builder.emptyNode.size should be === 0
	}

	"A node after insertion" should "contain 1 entry" in {
		val newNode = builder.newNode(1, "Value")
		newNode.keys.length should be === 1
	}

	"A node after value insertion" should "contain 1 entry" in {
		val newNode = builder.newNode(1, "Value")
		val addition = builder.insertValue(newNode, 2, "AnotherValue")
		addition.node.keys.length should be === 2
		addition.node.keys should be === Seq(1,2)
	}

	"A node after value insertions" should "contain entries in order" in {
		val one = builder.newNode(1, "One")
		val two = builder.insertValue(one, 3, "Three")
		val three = builder.insertValue(two.node, 2, "Two")
		three.node.keys.length should be === 3
		three.node.keys should be === Seq(1,2,3)
		three.node.values should be === Seq("One", "Two", "Three")
	}


}