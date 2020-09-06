from chispa.column_comparer import *
import pyspark.sql.functions as F
from code.src import StandardizeData
import traceback
from sparktestingbase.sqltestcase import *


def test_remove_non_word_characters_short():
    data = [
        ("jo&&se", "jose"),
        ("**li**", "li"),
        ("#::luisa", "luisa"),
        (None, None)
    ]
    df = spark.createDataFrame(data, ["name", "expected_name"])\
        .withColumn("clean_name", remove_non_word_characters(F.col("name")))