ThisBuild / version := "1.1.0"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "deltatables-optimizations"
  )

val sparkVersion = "3.3.2"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "io.delta" %% "delta-core" % "2.2.0" % "provided",
  "org.wvlet.airframe" %% "airframe-log" % "22.7.3",
  "com.github.scopt" %% "scopt" % "4.1.0"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x                             => MergeStrategy.first
}
