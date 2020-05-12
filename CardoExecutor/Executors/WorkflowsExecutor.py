from itertools import chain
from typing import Iterator, List, Union

import networkx as nx
from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Common.CardoGraph import CardoGraph
from CardoExecutor.Contract.CardoContextBase import CardoContextBase
from CardoExecutor.Contract.IWorkflowExecutor import IWorkflowExecutor
from CardoExecutor.Workflows.DagWorkflow import DagWorkflow
from CardoExecutor.Workflows.WorkflowsFactory import WorkflowsFactory


class WorkflowsExecutor:
    """
    Execute multiple workflows
    """
    def __init__(self, executor: IWorkflowExecutor, max_fails: int = 0, cannot_fail: List[str] = ()):
        """
        :param executor: the executor to run the workflows. ex: LinearWorkflowExecutor
        :param max_fails: max workflows that can fail without failing the run. put `-1` for unlimited failures.
        :param cannot_fail: if max_fails is used, you can specify a workflow that if it fails everything fails.
        """
        self.executor = executor
        self.max_fails = max_fails
        self.cannot_fail = cannot_fail

    def execute_workflows(self, workflows: WorkflowsFactory, cardo_context: CardoContextBase, max_fails: int = None) \
            -> Iterator[Union[CardoDataFrame, None]]:
        max_fails = max_fails if max_fails else self.max_fails
        results = []
        fail_count = 0
        workflows_to_run = list(workflows.get_workflows_to_run())
        self.__log_workflows_plan(workflows_to_run, cardo_context)
        for workflow in workflows_to_run:
            try:
                results.append(list(self.executor.execute(workflow, cardo_context)))
            except Exception as e:
                fail_count += 1
                cardo_context.logger.error(e)
                if (fail_count >= max_fails) and (max_fails >= 0):
                    raise Exception(f"Workflow errors exceeded max fails limit ({max_fails})")
                elif workflow.name in self.cannot_fail:
                    raise Exception(f"{e}")
        return chain.from_iterable(results)

    @staticmethod
    def __log_workflows_plan(workflows: List[DagWorkflow], cardo_context: CardoContextBase) -> None:
        dg = CardoGraph()
        for workflow in workflows:
            dg = nx.compose(dg, workflow.dag)
        cardo_context.logger.debug(f"Entire Workflows plan: {str(dg)}")

