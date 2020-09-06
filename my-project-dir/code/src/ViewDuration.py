from pyspark.sql import functions as f
from pyspark.sql.window import Window
from pyspark.sql.dataframe import *
from SparkSessionCreate import SparkSessionCreate


class PageViewDuration:
    @staticmethod
    def calculate_duration(df):
        window_spec = Window.partitionBy("brochure_click_uuid").orderBy("date_time")
        time_fmt = 'yyyy-MM-dd HH:mm:ss'
        end_time = f.lead(f.col("date_time"), 1).over(window_spec)
        start_time = f.col("date_time")
        time_diff = (f.unix_timestamp(end_time, time_fmt)
                     - f.unix_timestamp(start_time, time_fmt))
        duration_df = df.withColumn("duration",
                                    f.lit(f.when(f.col("event") == "EXIT_VIEW", f.lit(None).cast("long"))
                                          .otherwise(time_diff))) \
            .where(
            "event != 'EXIT_VIEW'") \
            .select(f.col("brochure_click_uuid"), f.to_timestamp(f.col("date_time")).alias("date_time"),
                    f.col("page"), f.col("page_view_mode"),
                    f.col("event"), f.col("duration")) \
            .filter(f.col("duration").isNotNull())
        return duration_df


if __name__ == "__main__":
    spark = SparkSessionCreate().get_spark_session("testing")
    df1 = spark.read.format("json").option("inferSchema", True).load(
        "/home/hadoop/my-project-dir/my-project-dir/data/page_turns.json")
    df1.printSchema()
    df2 = PageViewDuration.calculate_duration(df1)
    df2.printSchema()
    df2.show(truncate=False)
