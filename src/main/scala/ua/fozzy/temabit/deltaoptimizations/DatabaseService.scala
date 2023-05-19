package ua.fozzy.temabit.deltaoptimizations

import ua.fozzy.temabit.deltaoptimizations.Main.Config
import ua.fozzy.temabit.deltaoptimizations.SparkSession.spark
import wvlet.log.LogSupport

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

object DatabaseService extends LogSupport {
  def collectObjects(config: Config): List[String] = {
    import spark.implicits._
    var objects = ListBuffer[String]()
    val databases = ListBuffer[String]()
    databases.appendAll(
      spark.sql(s"show databases like '*'").as[String].collect()
    )
    logger.debug(s"Databases found: ${databases.mkString(", ")}")
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
    logger.debug(s"Objects found: ${objects.mkString(", ")}")
    if (!config.include.head.equals("all")) {
      val regexp = new Regex(
        config.include
          .map(x => "^" + x + (if (x.contains(".")) "$" else "\\..+"))
          .mkString("|")
      )
      logger.debug(
        s"Using regexp '${regexp.toString()}' to filter objects that should be included"
      )
      objects = objects.filter(obj => regexp.pattern.matcher(obj).matches())
    }

    if (config.exclude.nonEmpty) {
      val regexp = new Regex(
        config.exclude
          .map(x => "^" + x + (if (x.contains(".")) "$" else "\\..+"))
          .mkString("|")
      )
      logger.debug(
        s"Using regexp '${regexp.toString()}' to filter objects that should be excluded"
      )
      objects = objects.filter(obj => !regexp.pattern.matcher(obj).matches())
    }
    val finalObjList = objects
      .filter(table => {
        var isDelta = false
        try {
          val format = spark
            .sql(s"""DESCRIBE DETAIL $table""")
            .select("format")
            .head()
            .getString(0)
          isDelta = format == "delta"
        } catch {
          case e: org.apache.spark.sql.delta.DeltaAnalysisException =>
            logger.info(s"$table is not a Delta table")
          case e: org.apache.spark.sql.catalyst.analysis.NoSuchTableException =>
            logger.info(
              s"Table or view $table not found, while trying to get meta information about it"
            )
          case other: Throwable =>
            throw other
        }
        isDelta
      })
      .toList

    logger.debug(s"Final list of objects: ${finalObjList.mkString(", ")}")

    finalObjList

  }
}
