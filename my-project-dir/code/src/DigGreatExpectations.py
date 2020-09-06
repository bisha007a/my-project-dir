from great_expectations.core import ExpectationSuite
from pyspark.sql import SparkSession
import great_expectations as ge
from great_expectations.dataset import (sparkdf_dataset as sp, SqlAlchemyDataset)
from great_expectations.dataset import (SparkDFDataset)

expectation_suite = ExpectationSuite(
    expectation_suite_name='json_test_expectations',
    expectations=[
        {
            'expectation_type': 'expect_column_values_to_not_be_null',
            'kwargs': {
                'column': 'brochure_click_uuid'
            }
        },
        # I'm introducing the check on a not existing column on purpose
        # to see what types of errors are returned
        {
            'expectation_type': 'expect_column_to_exist',
            'kwargs': {
                'column': 'brochure_click_uuid'
            }
        }
    ]
)

expectation_suite_1 = ExpectationSuite(
    expectation_suite_name='data_testing_source',
    expectations=[
        {
            "expectation_type": "expect_table_row_count_to_equal",
            "kwargs": {
                "value": 359
            },
            "meta": {}
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "brochure_click_uuid"
            },
            "meta": {}
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "date_time"
            },
            "meta": {}
        },
        {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {
                "column": "brochure_click_uuid"
            },
            "meta": {}
        },
        {
            "expectation_type": "expect_column_values_to_be_of_type",
            "kwargs": {
                "column": "page",
                "type_": "LongType"
            },
            "meta": {}
        }
    ]
)
# new =
spark = SparkSession.builder.master("local[2]") \
    .appName("JSON loader").getOrCreate()
# /home/hadoop/Desktop/page_turns.json
sc = spark.sparkContext
json_dataset = spark.read.json('/home/hadoop/my-project-dir/my-project-dir/data',
                               lineSep='\n')
json_dataset1 = spark.read.format("json").load("/home/hadoop/my-project-dir/my-project-dir/data/page_turns.json")
kwargs_1 = {"persist": True}
context = ge.data_context.DataContext()
listing = sp.SparkDFDataset(json_dataset)
batch_kwargs = {'datasource': 'spark-based', 'dataset': json_dataset1}

batch = context.get_batch(batch_kwargs, expectation_suite)

batch_2 = context.get_batch(batch_kwargs, expectation_suite_1)

results = context.run_validation_operator(
    'action_list_operator', assets_to_validate=[batch_2], run_id='test_run'
)
print(type(results))
