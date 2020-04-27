from abc import ABCMeta, abstractmethod
from typing import Optional, List, Any

from CardoExecutor.Contract.IWorkflowFactory import IWorkflowFactory
from CardoExecutor.Workflows.DagWorkflow import DagWorkflow


class WorkflowsFactory(IWorkflowFactory, metaclass=ABCMeta):
    def __init__(self,
                 wanted_workflows: Optional[List[str]] = None,
                 unwanted_workflows: Optional[List[str]] = None,
                 should_write: bool = False):
        """
        :param wanted_workflows: run specific workflows
        :param unwanted_workflows: don't run specific workflows
        :param should_write: write the final result to hive (tables name are the workflow name)
        """
        self.wanted_workflows = wanted_workflows
        self.unwanted_workflows = unwanted_workflows
        self.should_write = should_write

    def get_workflows_to_run(self) -> List[DagWorkflow]:
        workflows = self.get_all_workflows()

        if self.wanted_workflows is not None:
            workflows = [workflow for workflow in workflows if workflow.name in self.wanted_workflows]

        if self.unwanted_workflows is not None:
            workflows = [workflow for workflow in workflows
                         if workflow.name not in self.unwanted_workflows]

        return workflows

    @abstractmethod
    def get_all_workflows(self) -> List[DagWorkflow]:
        raise NotImplementedError

    @abstractmethod
    def create_workflows(self, *args: Any, **kwargs: Any) -> DagWorkflow:
        raise NotImplementedError
