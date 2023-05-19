package ua.fozzy.temabit.deltaoptimizations

import scopt.OParser
import ua.fozzy.temabit.commands.{Optimize, Vacuum}
import wvlet.log.{LogLevel, LogSupport, Logger}

object Main extends LogSupport {
  case class Config(
      debug: Boolean = false,
      mode: String = "",
      exclude: Seq[String] = Seq(),
      include: Seq[String] = Seq("all"),
      optimizeCondition: Option[String] = None
  )

  def main(args: Array[String]): Unit = {

    val parser = ConfigParser.parser

    OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        if (config.debug) {
          Logger("ua.fozzy.temabit").setLogLevel(LogLevel.DEBUG)
        }
        val objects = DatabaseService.collectObjects(config)
        config.mode match {
          case "vacuum"   => Vacuum.run(objects)
          case "optimize" => Optimize.run(objects, config.optimizeCondition)
        }

      case _ =>
        logger.error("Provided arguments are not correct")

    }

  }
}
