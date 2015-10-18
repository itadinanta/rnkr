package net.itadinanta.rnkr.frontend

import scala.annotation.implicitNotFound
import scala.language.postfixOps

import spray.routing.directives.AuthMagnet.fromContextAuthenticator

trait ServiceVersion {
	def version: String
}