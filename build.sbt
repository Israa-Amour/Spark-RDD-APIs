version in ThisBuild := "0.1.0-SNAPSHOT"

scalaVersion in ThisBuild :="2.12.15"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.5.0"
libraryDependencies += "log4j" % "log4j" % "1.2.17"

lazy val root = (project in file("."))
  .settings(
    name := "untitled3"
  )


