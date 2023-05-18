import scopt.OParser
import wvlet.log.LogSupport

object Main extends LogSupport {
  case class Config(
      runVacuum: Boolean = false,
      exclude: Seq[String] = Seq(),
      include: Seq[String] = Seq("all")
  )

  def main(args: Array[String]): Unit = {

    val parser = ConfigParser.parser

    OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        val objects = DatabaseService.collectObjects(config)
        if (config.runVacuum) {
          Vacuum.run(objects)
        }

      case _ =>
        logger.error("Provided arguments are not correct")

    }

  }
}
