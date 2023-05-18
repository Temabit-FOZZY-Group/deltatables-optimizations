import Main.Config

import scala.collection.mutable.ListBuffer
import SparkSession.spark
import wvlet.log.LogSupport

import scala.util.matching.Regex
object Vacuum extends LogSupport{
  def run(config: Config): Unit = {
    import spark.implicits._
    var objects = ListBuffer[String]()
    val databases = ListBuffer[String]()
    databases.appendAll(
      spark.sql(s"show databases like '*'").as[String].collect()
    )
    databases.par
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
    if (!config.includeVacuum.head.equals("all")) {
      val regexp = new Regex(
        config.includeVacuum
          .map(x => "^" + x + (if (x.contains(".")) "$" else "\\..+"))
          .mkString("|")
      )
      objects = objects.filter(obj => regexp.pattern.matcher(obj).matches())
    }

    if (config.excludeVacuum.nonEmpty) {
      val regexp = new Regex(
        config.excludeVacuum
          .map(x => "^" + x + (if (x.contains(".")) "$" else "\\..+"))
          .mkString("|")
      )
      objects = objects.filter(obj => !regexp.pattern.matcher(obj).matches())
    }

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
  }
}
