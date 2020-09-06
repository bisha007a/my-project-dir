from pyspark.sql import types as t


def schemaCommonToInputDataset():
    schema = t.StructType([
        t.StructField("brochure_click_uuid", t.StringType(), True),
        t.StructField("date_time", t.StringType(), True),
        t.StructField("page", t.LongType(), True),
        t.StructField("page_view_mode", t.StringType(), True),
        t.StructField("event", t.StringType(), True)
    ])
    return schema


def ouputSchema():
    schema = t.StructType([
        t.StructField("brochure_click_uuid", t.StringType(), True),
        t.StructField("date_time", t.TimestampType(), True),
        t.StructField("page", t.LongType(), True),
        t.StructField("page_view_mode", t.StringType(), True),
        t.StructField("event", t.StringType(), True),
        t.StructField("duration", t.LongType(), True)
    ])
    return schema
