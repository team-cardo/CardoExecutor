from datetime import datetime

import re
from CardoLibs.IO import HiveWriter
from CardoLibs.IO.Hive.HiveReader import HiveReader

from CardoExecutor.Contract.IStep import step_to_hash_dict, hash_to_df_dict, IStep
from CardoExecutor.Contract.IStepRuntimeWrapper import IStepRuntimeWrapper

FULL_TABLE_NAME_TEMPLATE = '{ds_schema}.{table_name_template}_{timestamp}'

HIVE_SCHEMA_RECYCLE_CONFIG = 'cardo.recycle.schema'

SAVE_RESULTS_AT_RUNTIME_CONFIG = 'cardo.recycle.saveResultsAtRuntime'

TABLE_NAME_TEMPLATE = '{step_name}_{step_hash}'

TABLE_NAME_COLUMN = 'tableName'

DEFAULT_SCHEMA = 'your_schema'

TIMESTAMP_FORMAT = '%Y_%m_%dT%H_%M_%S'

LOG_TEMPLATE = 'step {step_name} with hash {hash} and parameters: {step_params} to {ds_schema}.{table_name}'

loaded_steps = set()


class RecycleStepRuntimeWrapper(IStepRuntimeWrapper):
    def __init__(self, step):
        # type: (IStep) -> None
        super(RecycleStepRuntimeWrapper, self).__init__(step)
        self.schema = None

    def process(self, cardo_context, *cardo_dataframe):
        if step_to_hash_dict[self.step] in hash_to_df_dict:
            return hash_to_df_dict[step_to_hash_dict[self.step]]
        self.schema = cardo_context.spark.conf.get(HIVE_SCHEMA_RECYCLE_CONFIG, DEFAULT_SCHEMA)
        if self.step.recompute:
            return self.compute(cardo_context, *cardo_dataframe)
        return self.load(cardo_context, *cardo_dataframe)

    def save(self, cardo_context, result):
        table_name = FULL_TABLE_NAME_TEMPLATE.format(ds_schema=self.schema,
                                                     table_name_template=self.step_table_name_template(self.step),
                                                     timestamp=datetime.now().strftime(TIMESTAMP_FORMAT))
        HiveWriter(table_name).process(cardo_context, result)
        cardo_context.logger.info(
            'Saved ' + LOG_TEMPLATE.format(step_name=self.step.name, hash=step_to_hash_dict[self.step].hex(),
                                           step_params=self.step.__dict__, ds_schema=self.schema,
                                           table_name=table_name))

    def compute(self, cardo_context, *cardo_dataframe):
        result = self.step.process(cardo_context, *cardo_dataframe).persist()
        hash_to_df_dict[step_to_hash_dict[self.step]] = result
        if self.step.rename:
            result.table_name = self.step.rename
        if cardo_context.spark.conf.get(SAVE_RESULTS_AT_RUNTIME_CONFIG, 'True') == 'True':
            self.save(cardo_context, result)
        return result

    @staticmethod
    def step_table_name_template(step):
        return TABLE_NAME_TEMPLATE.format(step_name=step.__class__.__name__, step_hash=step_to_hash_dict[step].hex())

    def load(self, cardo_context, *cardo_dataframe):
        latest_result_table_name = self._get_latest_result(cardo_context)
        if latest_result_table_name:
            result = HiveReader(latest_result_table_name).process(cardo_context).persist()
            loaded_steps.add(self.step)
            cardo_context.logger.info(
                'Loaded ' + LOG_TEMPLATE.format(step_name=self.step.name, hash=step_to_hash_dict[self.step].hex(),
                                                step_params=self.step.__dict__, ds_schema=self.schema,
                                                table_name=latest_result_table_name))
            if self.step.rename:
                result.table_name = self.step.rename
        else:
            result = self.compute(cardo_context, *cardo_dataframe)
        hash_to_df_dict[step_to_hash_dict[self.step]] = result
        return result

    def _table_name_to_timestamp(self, table_name):
        table_name_timestamp_regex = '{}_(.*)'.format(self.step_table_name_template(self.step))
        return datetime.strptime(re.sub(table_name_timestamp_regex, '\\1', table_name), TIMESTAMP_FORMAT)

    def _get_latest_result(self, cardo_context):
        previous_results = cardo_context.spark.sql(
            "show tables in {schema} like '{table_name_template}*'".format(schema=self.schema,
                                                                           table_name_template=self.step_table_name_template(
                                                                               self.step))).select(
            TABLE_NAME_COLUMN).collect()
        if previous_results:
            previous_results_table_names = [row[TABLE_NAME_COLUMN] for row in previous_results]
            table_names_and_timestamps = [(table_name, self._table_name_to_timestamp(table_name)) for table_name in
                                          previous_results_table_names]
            latest_result = max(table_names_and_timestamps, key=lambda table_timestamp_tuple: table_timestamp_tuple[1])
            latest_table_name, latest_table_timestamp = latest_result
            if (datetime.now() - latest_table_timestamp) > self.step.load_delta_limit:
                return latest_table_name
