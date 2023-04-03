ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "SparkScala"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-sql" % "2.4.8",
  "org.apache.spark" %% "spark-streaming" % "2.4.8",
  "com.typesafe" % "config" % "1.4.2",
  "org.postgresql" % "postgresql" % "42.2.5"
)

resourceDirectory in Compile := baseDirectory.value / "conf"