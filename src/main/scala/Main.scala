import org.apache.spark.sql.SparkSession
import scopt.OParser
import wvlet.log.LogSupport

import scala.collection.mutable.ListBuffer

object Main extends LogSupport {
  case class Config(
      databases: Seq[String] = Seq(),
      excludeDb: Seq[String] = Seq()
  )

  def main(args: Array[String]): Unit = {
    val builder = OParser.builder[Config]
    val parser = {
      import builder._
      OParser.sequence(
        programName("vacuum-runner"),
        head("vacuum", "0.1"),
        opt[Seq[String]]('d', "databases")
          .valueName("<db1>,<db2>...")
          .action((x, c) => c.copy(databases = x))
          .text("A list of databases to run VACUUM on"),
        opt[Seq[String]]('e', "excludeDb")
          .valueName("<db1>,<db2>...")
          .action((x, c) => c.copy(excludeDb = x))
          .text("A list of databases to exclude from VACUUM"),
        help("help").text("use this jar with spark")
      )
    }

    OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        val spark: SparkSession = SparkSession
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

        import spark.implicits._
        val objects = ListBuffer[String]()
        val databases = ListBuffer[String]()
        val databasesToExclude = ListBuffer[String]()

        if (!config.databases.head.equals("all")) {
          config.databases.foreach(db => {
            databases.appendAll(
              spark.sql(s"show databases like '$db'").as[String].collect()
            )
          })
        } else {
          databases.appendAll(
            spark.sql(s"show databases like '*'").as[String].collect()
          )
        }
        config.excludeDb.foreach(db => {
          databasesToExclude.appendAll(
            spark.sql(s"show databases like '$db'").as[String].collect()
          )
        })

        val dbs = databases.diff(databasesToExclude)

        dbs.par
          .foreach(db => {
            val tables = spark
              .sql(s"""show tables in $db""")

            objects.appendAll(
              tables
                .map(row => s"${row.getString(0)}.${row.getString(1)}")
                .as[String]
                .collect()
            )

          })

        objects.foreach(table => {
          try {
            val format = spark
              .sql(s"""DESCRIBE DETAIL $table""")
              .select("format")
              .head()
              .getString(0)
            if (format == "delta") {
              logger.info(s"running vacuum on '$table''")
              spark.sql(s"""VACUUM $table""");
              logger.info(s"finished vacuum on '$table''")
            }
          } catch {
            case e: Exception =>
              logger.warn(
                s"Error occurred while processing '$table', ${e.getLocalizedMessage}"
              )
          }
        })

      case _ =>
        logger.error("Provided arguments are not correct")

    }

  }
}
