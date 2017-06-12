import scala.io

name := "pyspark-cassandra"

version := io.Source.fromFile("version.txt").mkString.trim

organization := "anguenot"

scalaVersion := "2.11.6"

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

libraryDependencies ++= Seq(
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.2",
  "net.razorvine" % "pyrolite" % "4.10"
)

spName := "anguenot/pyspark-cassandra"

sparkVersion := "2.1.1"

sparkComponents ++= Seq("core", "streaming", "sql")

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(
  includeScala = false
)

EclipseKeys.withSource := true

spIgnoreProvided := true

ivyScala := ivyScala.value map {
  _.copy(overrideScalaVersion = true)
}
