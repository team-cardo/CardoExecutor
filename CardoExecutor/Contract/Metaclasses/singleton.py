INSTANCE = "__instance"


class Singleton(type):
	"""
	When used as a metaclass, force a class to have only 1 instance
	creating the class again will return the existing instance.
	Although you can delete it when calling class.destroy()

	Examples
	--------
	class MyClass(metaclass=Singleton):
		def __init__(self,arg1)
			self.arg1 = arg1

	a = MyClass(1)
	b = MyClass(2)
	print(a.arg1)
	...> 1
	print(b.arg1)
	...> 1

	MyClass.destroy()
	c = MyClass(3)
	print(c.arg1)
	...> 3

	NOTE
	----
	When destroying you need to call it from the class not from the instance.
	"""

	def __call__(cls, *args, **kwargs):
		if not hasattr(cls, INSTANCE):
			setattr(cls, INSTANCE, super().__call__(*args, **kwargs))
		return getattr(cls, INSTANCE)

	def destroy(cls):
		if hasattr(cls, INSTANCE):
			delattr(cls, INSTANCE)
