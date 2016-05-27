name := "sql2gcloud"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  //the commons
  "com.google.guava" % "guava" % "19.0",
  "io.atlassian.fugue" % "fugue" % "3.1.0",
  "io.atlassian.fugue" % "fugue-guava" % "3.1.0",
  "org.apache.commons" % "commons-lang3" % "3.4",

  // json
  "com.fasterxml.jackson.core" % "jackson-core" % "2.7.3",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.3",

  // gcloud storage
  "com.google.cloud" % "gcloud-java-storage" % "0.2.0",

  // sql driver
  "mysql" % "mysql-connector-java" % "5.1.38",

  // rx
  "io.reactivex" % "rxjava" % "1.1.0",

  // logging
  "org.apache.logging.log4j" % "log4j-core" % "2.5",
  "org.apache.logging.log4j" % "log4j-api" % "2.5",
  "com.lmax" % "disruptor" % "3.3.2"
)

mainClass in (Compile, assembly) := Some("nl.hayovanloon.gcp.sql2gcloud.Runner")