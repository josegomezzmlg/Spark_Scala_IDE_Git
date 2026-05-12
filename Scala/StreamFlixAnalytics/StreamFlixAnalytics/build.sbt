ThisBuild / version := "0.1"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "StreamFlixAnalytics",
    idePackagePrefix := Some("com.streamflix"),

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.3.0",
      "org.apache.spark" %% "spark-sql" % "3.3.0"
    )
  )