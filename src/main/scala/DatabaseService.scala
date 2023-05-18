import Main.Config
import SparkSession.spark

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

object DatabaseService {
  def collectObjects(config: Config): List[String] = {
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
    if (!config.include.head.equals("all")) {
      val regexp = new Regex(
        config.include
          .map(x => "^" + x + (if (x.contains(".")) "$" else "\\..+"))
          .mkString("|")
      )
      objects = objects.filter(obj => regexp.pattern.matcher(obj).matches())
    }

    if (config.exclude.nonEmpty) {
      val regexp = new Regex(
        config.exclude
          .map(x => "^" + x + (if (x.contains(".")) "$" else "\\..+"))
          .mkString("|")
      )
      objects = objects.filter(obj => !regexp.pattern.matcher(obj).matches())
    }
    objects.toList
  }
}
