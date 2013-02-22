name := "ace"

organization := "flatMap"

version := "1.1"

scalaVersion := "2.10.0"

parallelExecution in Test := false

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.10" % "1.9.1" % "test",
  "org.neo4j.app" % "neo4j-server" % "1.9.M04" classifier "" classifier "static-web",
  "com.sun.jersey" % "jersey-core" % "1.9"
)