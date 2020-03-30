from datetime import timedelta

from CardoExecutor.Contract.IStep import IStep


class IRecycleStep(IStep):
    def __init__(self, name=None, recompute=False, load_delta_limit=None, rename=None):
        # type: (str, bool, timedelta, str) -> None
        super(IRecycleStep, self).__init__(name, recompute, load_delta_limit, rename)
