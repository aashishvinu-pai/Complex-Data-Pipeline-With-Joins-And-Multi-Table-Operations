name := "nyc-taxi-iceberg"
version := "1.0"
scalaVersion := "2.13.17"

val sparkVersion = "3.5.5"
val icebergVersion = "1.9.0"

libraryDependencies ++= Seq(
  // Spark dependencies - provided if running on cluster
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",

  // Iceberg runtime for Spark 3.5
  "org.apache.iceberg" %% "iceberg-spark-runtime-3.5" % icebergVersion,

  // Your internal util library
  "sds-pe-core" %% "sds-pe-core" % "1.0.0",

  // Logging
  "ch.qos.logback" % "logback-classic" % "1.5.12"
)

// Allow sbt to find your locally published SDS artifact
resolvers += Resolver.mavenLocal
resolvers += "Maven Central" at "https://repo1.maven.org/maven2/"

// Assembly settings
import sbtassembly.AssemblyPlugin.autoImport._
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf"              => MergeStrategy.concat
  case x if x.endsWith(".class") && x.contains("scala/") => MergeStrategy.discard
  case x if x.contains("scala/")     => MergeStrategy.discard
  case _                             => MergeStrategy.first
}