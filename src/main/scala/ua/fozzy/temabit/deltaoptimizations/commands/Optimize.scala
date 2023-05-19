package ua.fozzy.temabit.commands

import ua.fozzy.temabit.deltaoptimizations.SparkSession.spark
import wvlet.log.LogSupport
object Optimize extends LogSupport {
  def run(objects: List[String], condition: Option[String]): Unit = {

    objects.foreach(table => {
      try {
        logger.info(
          s"running 'OPTIMIZE $table WHERE ${condition.getOrElse("1=1")}'"
        )
        spark.sql(s"""OPTIMIZE $table WHERE ${condition.getOrElse("1=1")}""");
        logger.info(s"finished optimize on '$table'")
      } catch {
        case e: Exception =>
          logger.warn(
            s"Optimize failed on '$table', ${e.getLocalizedMessage}"
          )
      }
    })
  }
}
