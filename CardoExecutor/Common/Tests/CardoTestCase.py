import unittest

from pyspark import Row
from pyspark.sql.types import *

from CardoExecutor.Common.Factory.CardoContextFactory import CardoContextFactory

CONTEXT_CONFIG = {"app_name": "unittest", "environment": "dev", "partitions": 1, 'append_spark_config': {'spark.jars': ''}}


class CardoTestCase(unittest.TestCase):
	@classmethod
	def setUpClass(cls):
		cls.context = CardoContextFactory().create_context(cluster='local', **CONTEXT_CONFIG)

	def convert_row_to_dict(self, row):
		if isinstance(row, Row):
			return self.convert_row_to_dict(row.asDict())
		if isinstance(row, dict):
			for key in row:
				row[key] = self.convert_row_to_dict(row[key])
			return row
		if isinstance(row, list):
			return sorted([self.convert_row_to_dict(item) for item in row])
		return row

	def convert_schema_to_list(self, schema):
		if isinstance(schema, list):
			return [self.convert_schema_to_list(item) for item in schema]
		if isinstance(schema, StructType):
			return self.convert_schema_to_list(schema.fields)
		if isinstance(schema, StructField):
			return [schema.name, self.convert_schema_to_list(schema.dataType)]
		if isinstance(schema, ArrayType):
			return [self.convert_schema_to_list(item) for item in schema.elementType]
		return schema

	def assertDataFramesEqual(self, expected, result, check_columns_order=False):
		self.assertItemsEqual(self.convert_row_to_dict(expected.collect()), self.convert_row_to_dict(result.collect()))
		if check_columns_order:
			self.assertListEqual(self.convert_schema_to_list(expected.schema), self.convert_schema_to_list(result.schema))
