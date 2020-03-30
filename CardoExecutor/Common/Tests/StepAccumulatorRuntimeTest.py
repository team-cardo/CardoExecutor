import pyspark.sql.functions as F
from pyspark.accumulators import AddingAccumulatorParam

from CardoExecutor.Common.Tests.CardoAccumulator import CardoAccumulator
from CardoExecutor.Contract.IStepRuntimeTest import IStepRuntimeTest


class StepAccumulatorRuntimeTest(IStepRuntimeTest):
    def __init__(self, func, name=None, columns=None, **kwargs):
        self.func = func
        self.name = (name or self.func.func_name)
        self.columns = columns
        self.kwargs = kwargs

    def __get_accumulator_dataframe_udf(self, cardo_context, cardo_dataframe, columns):
        accumulator = CardoAccumulator(cardo_context.logger, self.name, 0, AddingAccumulatorParam(0))

        def accumulator_func(column, *columns):
            if self.func(column, *columns, **self.kwargs):
                accumulator.add(1)
            return column

        return_type = cardo_dataframe.dataframe.schema[columns[0]].dataType
        return F.udf(lambda *columns: accumulator_func(*columns), return_type)

    def __get_final_columns(self, accumulator_columns, accumulator_udf, cardo_dataframe):
        select_columns = []
        for column in cardo_dataframe.dataframe.columns:
            if column != accumulator_columns[0]:
                select_columns.append(column)
            else:
                select_columns.append(accumulator_udf(*accumulator_columns).alias(accumulator_columns[0]))
        return select_columns

    def __accumulate_rdd(self, cardo_context, rdd):
        accumulator = CardoAccumulator(cardo_context.logger, self.name, 0, AddingAccumulatorParam(0))

        def accumulator_func(item):
            if self.columns is not None:
                relevant_columns = map(lambda column: item[column], self.columns)
                if self.func(*relevant_columns, **self.kwargs):
                    accumulator.add(1)
            else:
                accumulator.add(1)
            return item

        return rdd.map(accumulator_func)

    def __accumulate_dataframe(self, cardo_context, cardo_dataframe):
        accumulator_columns = self.columns or cardo_dataframe.dataframe.columns[:1]
        if len(accumulator_columns) == 0:
            return
        accumulator_udf = self.__get_accumulator_dataframe_udf(cardo_context, cardo_dataframe, accumulator_columns)
        select_columns = self.__get_final_columns(accumulator_columns, accumulator_udf, cardo_dataframe)
        cardo_dataframe.dataframe = cardo_dataframe.dataframe.select(select_columns)

    def test(self, cardo_context, cardo_dataframe):
        try:
            payload_type = cardo_dataframe.payload_type
        except AttributeError:
            return cardo_dataframe
        if payload_type == 'dataframe':
            self.__accumulate_dataframe(cardo_context, cardo_dataframe)
        if payload_type == 'rdd':
            cardo_dataframe.rdd = self.__accumulate_rdd(cardo_context, cardo_dataframe.rdd)
        return cardo_dataframe
