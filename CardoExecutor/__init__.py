from CardoExecutor.Common.Factory.CardoContextFactory import CardoContextFactory
from CardoExecutor.Executors.AsyncWorkflowExecutor import AsyncWorkflowExecutor
from CardoExecutor.Executors.LinearWorkflowExecutor import LinearWorkflowExecutor
from CardoExecutor.Workflows.DagSubWorkflow import DagSubWorkflow, DagWorkflow


def create_context(app_name, max_cores=1, executor_cores=1, executor_memory="5gb", append_spark_config=None,
                   environment="your_env1", append_log_config=None, partitions=200, logger=None, stream=False,
                   stream_interval=None, cluster="your_env2", *args, **kwargs):
    return CardoContextFactory().create_context(
        app_name, max_cores, executor_cores, executor_memory, append_spark_config, environment, append_log_config,
        partitions, logger, stream, stream_interval, cluster, *args, **kwargs)
