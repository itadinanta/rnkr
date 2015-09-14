package net.itadinanta.rknr.util

import org.scalatest.Matchers
import org.scalatest.FunSuite
import scala.util.Random
import scala.collection.mutable._
import grizzled.slf4j.Logging
import net.itadinanta.rnkr.util.SetOnce

class SetOnceTest extends FunSuite with Matchers with Logging {
	test("setOnce new") {
		val v = SetOnce[Int]
		v.isSet shouldBe false
		intercept[IllegalStateException] { v.get }
	}

	test("setOnce set") {
		val v = SetOnce[Int]
		v := 1
		v.isSet shouldBe true
		v.get shouldBe 1
		intercept[IllegalStateException] { v := 2 }
	}
}