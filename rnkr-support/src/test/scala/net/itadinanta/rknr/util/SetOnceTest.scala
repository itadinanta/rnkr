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

	test("setOnce toString") {
		val v = SetOnce[Int]
		v.toString shouldBe "_"
		v := 1
		v.toString shouldBe "1"
	}

	test("setOnce unset map") {
		val v = SetOnce[Int]
		val vmapped = v map (_ * 2)
		vmapped.isSet shouldBe false
	}

	test("setOnce set map") {
		val v = SetOnce[Int]
		v := 1
		val vmapped = v map (_ * 2)
		vmapped.isSet shouldBe true
		vmapped.get shouldBe 2
	}

	test("setOnce unset flatmap") {
		val v = SetOnce[Int]
		val vmapped = v flatMap (SetOnce[Int] := _ * 2)
		vmapped.isSet shouldBe false
	}

	test("setOnce set flatmap") {
		val v = SetOnce[Int]
		v := 1
		val vmapped = v flatMap (SetOnce[Int] := _ * 2)
		vmapped.isSet shouldBe true
		vmapped.get shouldBe 2
	}

	test("setOnce set for") {
		val v = SetOnce[Int]
		val vunmapped = for { value <- v } yield value * 2
		vunmapped.isSet shouldBe false
		
		v := 1
		val vmapped = for { value <- v } yield value * 2
		vmapped.isSet shouldBe true
		vmapped.get shouldBe 2
	}

}