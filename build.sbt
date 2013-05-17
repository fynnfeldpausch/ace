name := "ace"

version := "1.1"

scalaVersion := "2.10.1"

parallelExecution in Test := false

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.10" % "1.9.1" % "test",
  "org.neo4j.app" % "neo4j-server" % "1.9.RC2"
)