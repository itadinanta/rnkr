package net.itadinanta.rankr;

import net.itadinanta.rnkr.node._
import org.scalatest.FlatSpec
import org.scalatest.ShouldMatchers

class NodeTestSuite extends FlatSpec with ShouldMatchers {
	val builder = new NodeBuilder[Int, String, Record[String]](IntAscending)
	"An empty node" should "contain no entries" in {
		builder.emptyNode.size should be === 0
	}

	"A node after insertion" should "contain 1 entry" in {
		val newNode = builder.newNode(1, "Value")
		newNode.keys.length should be === 1
	}

	"A node after insertion" should "contain 1 entry" in {
		val newNode = builder.newNode(1, "Value")
		val addition = builder.insert(newNode, 2, "AnotherValue")
		addition.node.asInstanceOf[WithChildren].keys.length should be === 2
	}


}