import scala.io

name := "pyspark-cassandra"

version := io.Source.fromFile("version.txt").mkString.trim

organization := "anguenot"

scalaVersion := "2.12.18"

credentials ++= {
  val credFile = Path.userHome / ".ivy2" / ".sbtcredentials"
  if (credFile.exists()) Seq(Credentials(credFile)) else Seq.empty
}

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

val sparkVersion = "3.5.5"

libraryDependencies ++= Seq(
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.5.1",
  "net.razorvine" % "pyrolite" % "5.1",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)

javacOptions ++= Seq("-source", "17", "-target", "17")

scalacOptions ++= Seq("-unchecked", "-feature")

assembly / assemblyPackageScala / assemblyMergeStrategy := { _ => MergeStrategy.discard }

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", xs@_*) => MergeStrategy.last
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}