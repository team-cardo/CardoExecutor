from datetime import timedelta

from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Contract.CardoContextBase import CardoContextBase

step_to_hash_dict = {}
hash_to_df_dict = {}
ISTEP_NAME_FORMAT = '{class_name} with hash {object_hash}'


class IStep(object):
    def __init__(self, name=None, recompute=True, load_delta_limit=None, rename=None):
        # type: (str, bool, timedelta, str) -> None
        self.name = name or self.__class__.__name__
        self.recompute = recompute
        self.load_delta_limit = load_delta_limit or timedelta()
        self.rename = rename

    @staticmethod
    def _pm_decorator(columns, name, before_or_after, cardo_dataframe_index, kwargs):
        def wrapper(func):
            func.__pm__ = [columns, name, before_or_after, cardo_dataframe_index, kwargs]
            return func

        return wrapper

    @staticmethod
    def pm_input(columns=None, name=None, cardo_dataframe_index=None, **kwargs):
        return IStep._pm_decorator(columns, name, 'before', cardo_dataframe_index, kwargs)

    @staticmethod
    def pm_output(columns=None, name=None, cardo_dataframe_index=None, **kwargs):
        return IStep._pm_decorator(columns, name, 'after', cardo_dataframe_index, kwargs)

    def process(self, cardo_context, cardo_dataframe):
        # type: (CardoContextBase, CardoDataFrame) -> CardoDataFrame
        raise NotImplementedError()

    def __str__(self):
        if step_to_hash_dict.get(self):
            return ISTEP_NAME_FORMAT.format(class_name=self.name, object_hash=step_to_hash_dict.get(self).hex())
        else:
            return super(IStep, self).__str__()
