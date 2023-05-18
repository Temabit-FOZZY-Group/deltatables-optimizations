import scopt.OParser
import wvlet.log.LogSupport

object Main extends LogSupport {
  case class Config(
      runVacuum: Boolean = false,
      excludeVacuum: Seq[String] = Seq(),
      includeVacuum: Seq[String] = Seq("all")
  )

  def main(args: Array[String]): Unit = {

    val parser = ConfigParser.parser

    OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        if (config.runVacuum) {
          Vacuum.run(config)
        }
      case _ =>
        logger.error("Provided arguments are not correct")

    }

  }
}
