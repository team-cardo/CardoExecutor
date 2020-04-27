from abc import ABCMeta
from functools import wraps
from typing import Callable

from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Contract.CardoContextBase import CardoContextBase
from CardoExecutor.Contract.IStep import IStep
from CardoExecutor.Contract.IWrappers import lock, register, persist
from CardoExecutor.Contract.ILogicStep import ILogicStep


def cache(func: Callable) -> Callable:
    @lock()
    @register()
    @persist
    @wraps(func)
    def inner(self, cardo_context, *args, **kwargs):
        return func(self, cardo_context, *args, **kwargs)
    return inner


class ICacheStep(ILogicStep, metaclass=ABCMeta):
    """
    Run persist on the process output.
    The DataFramesFactory auto detect this class and make it accessible from everywhere.

    First option:
        class ExampleStep(ICacheStep):
            def __init__(self):
                super().__init__(reader=OracleReader(...))

    Second option:
        class ExampleStep(ICacheStep):
            @cache
            def process(self, cardo_context: CardoContextBase, cardo_dataframe: CardoDataFrame=None) -> CardoDataFrame:
                ...
                return CardoDataFrame
    """
    node_attributes = {'shape': 'rectangle'}

    def __new__(cls, *args, **kwargs):
        if cls is ICacheStep:
            raise TypeError("Can't initiate an interface")
        return super().__new__(cls, *args, **kwargs)

    def __init__(self, reader: IStep = None, **node_attributes):
        super().__init__(reader=reader, **dict(ICacheStep.node_attributes, **node_attributes))

    @cache
    def process(self, cardo_context: CardoContextBase, cardo_dataframe: CardoDataFrame = None) -> CardoDataFrame:
        return super().process(cardo_context, cardo_dataframe)
