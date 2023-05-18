import SparkSession.spark
import wvlet.log.LogSupport
object Vacuum extends LogSupport {
  def run(objects: List[String]): Unit = {

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
