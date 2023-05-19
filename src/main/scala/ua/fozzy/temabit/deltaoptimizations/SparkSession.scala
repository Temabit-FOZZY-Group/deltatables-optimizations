package ua.fozzy.temabit.deltaoptimizations

object SparkSession {
  val spark: org.apache.spark.sql.SparkSession =
    org.apache.spark.sql.SparkSession
      .builder()
      .appName("vacuum")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
      )
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )
      .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
      .config(
        "spark.serializer",
        "org.apache.spark.serializer.KryoSerializer"
      )
      .enableHiveSupport()
      .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
}
