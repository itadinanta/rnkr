package net.itadinanta.rnkr.frontend

import akka.actor.Actor
import spray.routing._
import spray.http._
import MediaTypes._
import spray.httpx.SprayJsonSupport.sprayJsonMarshaller
import spray.httpx.SprayJsonSupport.sprayJsonUnmarshaller
import spray.json.DefaultJsonProtocol

class ServiceActor extends Actor with Service {
	def actorRefFactory = context

	def receive = runRoute(rnkrRoute)
}

// this trait defines our service behavior independently from the service actor
trait Service extends HttpService with DefaultJsonProtocol {
	import spray.httpx.SprayJsonSupport.sprayJsonMarshaller
	import spray.httpx.SprayJsonSupport.sprayJsonUnmarshaller

	val HTML = `text/html`

	val rnkrRoute =
		path("hello") {
			get {
				respondWithMediaType(HTML) { // XML is marshalled to `text/xml` by default, so we simply override here
					complete {
						<html>
							<body>
								<h1>Say hello to <i>spray-routing</i> on <i>spray-can</i> </h1>
							</body>
						</html>
					}
				}
			}
		} ~ path("echo") {
			(post | get) {
				parameters('value.?) { (value) =>
					complete(transformed(value))
				}
			}
		} ~ pathPrefix("number") {
			path(IntNumber) { value =>
				complete(transformed(value))
			}
		} ~ pathPrefix("nestedValue") {
			pathPrefix("number") {
				path(IntNumber) { value =>
					complete(transformed(value))
				}
			} ~ pathPrefix("another") {
				path(IntNumber) { value =>
					complete(transformed(value))
				}
			}
		}

	def transformed(value: Option[String]) = value.getOrElse("null") + "_echoed"
	def transformed(value: Int) =
		(value + 1) toString

}