package ua.fozzy.temabit.deltaoptimizations

import scopt.OParser
import ua.fozzy.temabit.deltaoptimizations.Main.Config

object ConfigParser {
  val builder = OParser.builder[Config]
  val parser = {
    import builder._
    OParser.sequence(
      programName("deltatables-optimizations"),
      head("deltatables-optimizations", "0.1"),
      help("help").text(
        "use this jar via spark to perform vacuum on a desired table or database"
      ),
      opt[Unit]("debug")
        .action((_, c) => c.copy(debug = true)),
      opt[Seq[String]]('i', "include")
        .valueName("[optional] <db1>,<db2>,<db3>.<table1>...")
        .action((x, c) => c.copy(include = x))
        .text(
          s"""A list of objects to run VACUUM on.
             |By default, will run on all databases and tables in metastore
             |An example of accepted values:
             |user.* - all objects in databases that starts with "user"
             |user - all objects in user database
             |user_x.table - specific table
             |""".stripMargin
        ),
      opt[Seq[String]]('e', "exclude")
        .valueName("[optional] <db1>,<db2>,<db3>.<table1>...")
        .action((x, c) => c.copy(exclude = x))
        .text(s"""A list of databases to exclude from VACUUM.
             |An example of accepted values:
             |user.* - all objects in databases that starts with "user"
             |user - all objects in user database
             |user_x.table - specific table
             |""".stripMargin),
      cmd("vacuum")
        .action((_, c) => c.copy(mode = "vacuum"))
        .text("run vacuum on specific databases or tables"),
      cmd("optimize")
        .action((_, c) => c.copy(mode = "optimize"))
        .text("run optimize on specific databases or tables")
        .children(
          opt[String]('c', "condition")
            .valueName("[optional] (e.g. date >= '2017-01-01')")
            .action((x, c) => c.copy(optimizeCondition = Some(x)))
            .text(s"""A condition to run optimize with.
               |E.g. OPTIMIZE delta_table_name WHERE date >= '2017-01-01'
               |""".stripMargin)
        )
    )
  }
}
