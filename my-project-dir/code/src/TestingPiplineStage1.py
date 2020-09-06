from great_expectations.core import ExpectationSuite
from pyspark.sql import SparkSession
from src import ViewDuration as v
import great_expectations as ge

expectation_suite = ExpectationSuite(
    expectation_suite_name='testing_pipeline_debt',
    expectations=[
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "duration",
                "max_value": 600,
                "min_value": 0
            },
            "meta": {}
        }
    ]
)
spark = SparkSession.builder.master("local[2]") \
    .appName("JSON loader").getOrCreate()
json_dataset1 = spark.read.format("json").load("/home/hadoop/my-project-dir/my-project-dir/data/page_turns.json")
df2 = v.PageViewDuration.calculate_duration(json_dataset1)
context = ge.data_context.DataContext()
batch_kwargs = {'datasource': 'spark-based', 'dataset': df2}
batch = context.get_batch(batch_kwargs, expectation_suite)
results = context.run_validation_operator(
    'action_list_operator', assets_to_validate=[batch], run_id='test_run'
)
