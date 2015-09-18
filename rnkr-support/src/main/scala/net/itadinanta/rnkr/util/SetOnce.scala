package net.itadinanta.rnkr.util

object SetOnce {
	def apply[T]() = new SetOnce[T]()
}

final class SetOnce[T]() {
	private var _value: T = _
	private var _isSet: Boolean = false
	def :=(value: T): SetOnce[T] = if (_isSet) throw new IllegalStateException("value is already set") else {
		_value = value
		_isSet = true
		this
	}
	def isSet = _isSet
	def get = if (_isSet) _value else throw new IllegalStateException("value is not set")

	def map[R](f: T => R): SetOnce[R] = if (_isSet) SetOnce[R]() := f(_value) else SetOnce[R]()
	def flatMap[R](f: T => SetOnce[R]): SetOnce[R] = if (_isSet) f(_value) else SetOnce[R]()
	def foreach(f: T => Unit) { if (_isSet) f(_value) }

	override def toString() = if (_isSet) _value.toString else "_"
}
