import org.apache.spark.sql.SparkSession
import scopt.OParser
import wvlet.log.LogSupport

import java.util.concurrent.Executors
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, ExecutionContext, Future}

object Main extends LogSupport {
  case class Config(
      databases: Seq[String] = Seq(),
      threads: Int = 10
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
        opt[Int]('t', "threads")
          .action((x, c) => c.copy(threads = x))
          .text("Specify the parallelism"),
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

        import spark.implicits._
        val objects = ListBuffer[String]()

        spark
          .sql("show databases")
          .as[String]
          .filter(row =>
            config.databases.head.equalsIgnoreCase("all") || config.databases
              .contains(row)
          )
          .collect()
          .par
          .foreach(db => {
            val tables = spark.sql(s"""show tables in $db""")
            objects.appendAll(
              tables
                .map(row => s"${row.getString(0)}.${row.getString(1)}")
                .as[String]
                .collect()
            )
          })

        val parallelism = config.threads
        val executor = Executors.newFixedThreadPool(parallelism)
        implicit val ec: ExecutionContext =
          ExecutionContext.fromExecutor(executor)
        val results: Seq[Future[Any]] = objects.map(table => {
          Future {
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
                  s"Error occurred while processing '$table'\n ${e.getMessage}"
                )
            }
          }(ec)
        })

        val allDone: Future[Seq[Any]] = Future.sequence(results)
        Await.result(allDone, scala.concurrent.duration.Duration.Inf)
        executor.shutdown
      case _ =>
        logger.error("Provided arguments are not correct")

    }

  }
}
