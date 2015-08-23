package net.itadinanta.common

import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import scala.collection.JavaConversions._

abstract class GlobalConfig {
	val cfg: Config
	val root: String

	private def withKey[T](f: (String) => T, key: String): T = {
		val path = root + "." + key
		f(path)
//		if (cfg.hasPath(path)) Option(f(path)) else None
	}
	def string(key: String) = withKey(cfg.getString _, key)
	def strings(key: String) = withKey(cfg.getStringList(_).toList, key)
	def int(key: String) = withKey(cfg.getInt _, key)
	def boolean(key: String) = withKey(cfg.getBoolean _, key)
}

object GlobalConfig extends GlobalConfig {
	val root = Constants.NAMESPACE
	val cfg = ConfigFactory.load()
}
