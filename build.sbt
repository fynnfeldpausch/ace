name := "ace"

version := "1.0"

scalaVersion := "2.9.2"

parallelExecution in Test := false

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "1.8" % "test",
  "org.neo4j.app" % "neo4j-server" % "1.9.M03" classifier "" classifier "static-web",
  "com.sun.jersey" % "jersey-core" % "1.9"
)