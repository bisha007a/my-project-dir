import pytest
from chispa import *
from datetime import datetime
from src import ViewDuration as v
from schema import *
from conftest import spark


# Unit Testing

def test_page_view_duration(spark):
    # Input DF

    inputRow = [
        t.Row("0017a3da-f18a-4fc5-957e-d9d12200d01c", "2016-07-12 18:27:51",
              32, "SINGLE_PAGE_MODE", "EXIT_VIEW"),
        t.Row("0017a3da-f18a-4fc5-957e-d9d12200d01c", "2016-07-12 18:26:22",
              8, "DOUBLE_PAGE_MODE", "PAGE_TURN")
    ]

    inputRDD = spark.sparkContext.parallelize(inputRow)
    inputDF = spark.createDataFrame(inputRDD, schemaCommonToInputDataset()).cache()
    resultDF = v.PageViewDuration.calculate_duration(inputDF)

    # Expected DF

    date_time_str = '2016-07-12 18:26:22'
    date_time_obj = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
    expectedRows = [t.Row("0017a3da-f18a-4fc5-957e-d9d12200d01c",
                          date_time_obj, 8, "DOUBLE_PAGE_MODE", "PAGE_TURN", 89)]
    expectedRDD = spark.sparkContext.parallelize(expectedRows)
    expectedDF = spark.createDataFrame(expectedRDD, ouputSchema())
    assert_df_equality(expectedDF, resultDF)


test_page_view_duration(spark())
