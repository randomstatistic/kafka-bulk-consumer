import sbt._
import Keys._

name := "kafka-bulk-consumer"

version := "0.0.0"

scalaVersion := "2.10.2"

crossScalaVersions := Seq("2.10.2")

val ivyLocal = Resolver.file("local", file(Path.userHome.absolutePath + "/.ivy2/local"))(Resolver.ivyStylePatterns)

resolvers += ivyLocal

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % "0.8.1.1" exclude("com.sun.jdmk", "jmxtools") exclude("com.sun.jmx", "jmxri"),
  "com.typesafe.akka" %% "akka-actor" % "2.3.2",
  "com.typesafe.akka" %% "akka-testkit" % "2.3.2" % "test",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)
