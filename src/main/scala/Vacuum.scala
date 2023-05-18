import SparkSession.spark
import wvlet.log.LogSupport
object Vacuum extends LogSupport {
  def run(objects: List[String]): Unit = {

    objects.foreach(table => {
      try {
        logger.info(s"running vacuum on '$table'")
        spark.sql(s"""VACUUM $table""");
        logger.info(s"finished vacuum on '$table'")
      } catch {
        case e: Exception =>
          logger.warn(
            s"Vacuum failed on '$table', ${e.getLocalizedMessage}"
          )
      }
    })
  }
}
