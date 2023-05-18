import Main.Config
import scopt.OParser

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
      cmd("vacuum")
        .action((_, c) => c.copy(runVacuum = true))
        .text("run vacuum on specific databases or tables")
        .children(
          opt[Seq[String]]('i', "include")
            .valueName("<db1>,<db2>,<db3>.<table1>...")
            .action((x, c) => c.copy(includeVacuum = x))
            .text(
              s"""A list of objects to run VACUUM on.
                 |An example of accepted values:
                 |user.* - all objects in databases that starts with "user"
                 |user - all objects in user database
                 |user_x.table - specific table
                 |""".stripMargin
            ),
          opt[Seq[String]]('e', "exclude")
            .valueName("<db1>,<db2>,<db3>.<table1>...")
            .action((x, c) => c.copy(excludeVacuum = x))
            .text(s"""A list of databases to exclude from VACUUM.
                 |An example of accepted values:
                 |user.* - all objects in databases that starts with "user"
                 |user - all objects in user database
                 |user_x.table - specific table
                 |""".stripMargin)
        )
    )
  }
}
