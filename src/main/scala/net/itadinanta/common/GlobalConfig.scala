package net.itadinanta.common

import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config

object GlobalConfig {
	val cfg = ConfigFactory.load()
	val root = Constants.NAMESPACE
	private def withKey[T](f: (String) => T, key: String): Option[T] = {
		val path = root + "." + key
		if (cfg.hasPath(path)) Option(f(path)) else None
	}
	def getOptionalString(key: String) = withKey(cfg.getString _, key)
	def getOptionalInt(key: String) = withKey(cfg.getInt _, key)
	def getOptionalBoolean(key: String) = withKey(cfg.getBoolean _, key)
}