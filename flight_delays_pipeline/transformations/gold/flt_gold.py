from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="flt_gold",
    comment="Flight delay aggregation by carrier and delay bucket"
)
def flt_gold():
    return (
        spark.read.table("flt_silver")
        .withColumn(
            "delay_bucket",
            F.when(F.col("dep_delay_min") < 0, "early")
            .when(F.col("dep_delay_min").between(0, 15), "on_time")
            .when(F.col("dep_delay_min").between(16, 60), "short_delay")
            .when(F.col("dep_delay_min") > 60, "long_delay")
        )
        .groupBy("carrier", "delay_bucket")
        .agg(
            F.count("*").alias("flight_count"),
            F.avg("dep_delay_min").alias("avg_dep_delay_min")
        )
    )
