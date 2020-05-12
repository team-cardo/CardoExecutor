import functools
from threading import Lock
from typing import Callable

from CardoExecutor.Contract.CardoContextBase import CardoContextBase

LOCK_ATTRIBUTE = "lock"
DATA_ATTRIBUTE = "data"


def lock(lock_attribute: str = LOCK_ATTRIBUTE) -> Callable:
	"""
	Add a lock mechanism to the class if its not already exists.
	Use that lock whenever the function is called.
	"""

	def wrapper(func: Callable):
		@functools.wraps(func)
		def inner(self, *args, **kwargs):
			if not hasattr(self, lock_attribute):
				setattr(self, lock_attribute, Lock())
			with getattr(self, lock_attribute):
				return func(self, *args, **kwargs)

		return inner

	return wrapper


def register(attribute_name: str = DATA_ATTRIBUTE) -> Callable:
	"""
	Assign the return value of a function into the attribute_name attribute of a class if it doesn't exist already.
	Return the assigned value whenever the function is called again.
	If the attribute_name already exists it will just return it.
	"""

	def wrapper(func: Callable):
		@functools.wraps(func)
		def inner(self, *args, **kwargs):
			if not hasattr(self, attribute_name):
				setattr(self, attribute_name, func(self, *args, **kwargs))
			return getattr(self, attribute_name)

		return inner

	return wrapper


def persist(func: Callable) -> Callable:
	"""
	:param func: any process function that have CardoContext input and returns CardoDataFrame
	:return: persisted CardoDataFrame
	"""

	@functools.wraps(func)
	def inner(self, cardo_context: CardoContextBase, *args, **kwargs):
		return func(self, cardo_context, *args, **kwargs).persist()

	return inner
