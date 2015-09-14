package net.itadinanta.rnkr.util

object SetOnce {
	def apply[T]() = new SetOnce[T]()
}

class SetOnce[T]() {
	private var _value: T = _
	private var _isSet: Boolean = false
	def :=(value: T) = if (_isSet) throw new IllegalStateException("value is already set") else {
		_value = value
		_isSet = true
	}
	def isSet = _isSet
	def get() = if (_isSet) _value else throw new IllegalStateException("value is not set")
}
