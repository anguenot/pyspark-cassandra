import scala.io

name := "pyspark-cassandra"

version := io.Source.fromFile("version.txt").mkString.trim

organization := "anguenot"

scalaVersion := "2.11.12"

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

libraryDependencies ++= Seq(
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.2",
  "net.razorvine" % "pyrolite" % "4.30"
)

spName := "anguenot/pyspark-cassandra"

sparkVersion := "2.4.8"

sparkComponents ++= Seq("core", "streaming", "sql")
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(
  includeScala = false
)

spIgnoreProvided := true

ivyScala := ivyScala.value map {
  _.copy(overrideScalaVersion = true)
}

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", xs@_*) => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
