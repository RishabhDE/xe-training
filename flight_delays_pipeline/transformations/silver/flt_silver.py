from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="flt_silver",
    comment="Cleaned flight delay data - cancelled flights removed, types cast"
)
@dp.expect_or_drop("valid_delay", "dep_delay_min IS NOT NULL")
def flt_silver():
    return (
        spark.readStream.table("flt_bronze")
        .filter("cancelled != 'true' AND cancelled != '1'")
        .filter("carrier IS NOT NULL AND origin IS NOT NULL")
        .withColumn("dep_delay_min", F.col("dep_delay_min").cast("int"))
        .withColumn("arr_delay_min", F.col("arr_delay_min").cast("int"))
        .withColumn("flight_date", F.col("flight_date").cast("date"))
    )
