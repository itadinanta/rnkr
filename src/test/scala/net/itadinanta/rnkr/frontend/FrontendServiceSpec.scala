package net.itadinanta.rnkr.frontend

import spray.testkit.ScalatestRouteTest
import spray.http._
import StatusCodes._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.Matchers

class FrontendServiceSpec extends FunSuite with Matchers with ScalatestRouteTest with Service {
	def actorRefFactory = system
	val executionContext = system.dispatcher

	test("process a count request") {
		Get("/rnkr/tree/test/size") ~> rnkrRoute ~> check {
			handled should be(true)
			responseAs[String] === "0"
		}
	}
}
