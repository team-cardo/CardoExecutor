import logging

LEVEL_35 = "Level 35"


class WorkflowAndStepFilter(logging.Filter):
    def filter(self, record):
        splitted = record.name.split('.') + [None, None]
        record.workflow, record.step = splitted[1], splitted[2]
        return record


class Level35Filter(logging.Filter):
    def filter(self, record):
        return 0 if record.levelname == LEVEL_35 else 1


class NotLevel35Filter(logging.Filter):
    def filter(self, record):
        return 1 if record.levelname == LEVEL_35 else 0
