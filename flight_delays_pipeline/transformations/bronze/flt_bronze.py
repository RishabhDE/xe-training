from pyspark import pipelines as dp


@dp.table(
    name="flt_bronze",
    comment="Raw flight delay data ingested from daily CSV files via Auto Loader"
)
@dp.expect("row_count", "flight_date IS NOT NULL")
def flt_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("mergeSchema", "true")
        .load("/Volumes/xe_training_catalog/databricks/flights_landing/*.csv")
    )
