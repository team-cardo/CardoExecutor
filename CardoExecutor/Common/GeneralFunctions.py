from typing import Any, Callable, Generator

import pandas as pd


def get_all_subclasses(cls) -> Generator[Callable, Any, None]:
	"""
	:param cls:
	:return: all all classes that somewhere along the way inherit of the class
	"""
	for a_class in cls.__subclasses__():
		yield a_class
		for b_class in get_all_subclasses(a_class):
			yield b_class


def snake_case(camel_case: str) -> str:
	"""
	:param camel_case: CamelCase string
	:return: snake_case string
	"""
	tmp = pd.Series(list(camel_case))
	query = (tmp.str.isupper()) & (tmp.index > 0)
	tmp[query] = tmp[query].apply(lambda s: "_" + s)
	return ''.join(tmp.str.lower().tolist())
