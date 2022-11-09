import java.io.PrintWriter
import scala.io.Source
import scala.util.Try

logLevel := Level.Error

name := "Spooq"
version := "0.9.9beta"

val buildType = System.getProperty("build.spark.version", "3")
val standalone = Try(System.getProperty("standalone", "").toBoolean).getOrElse(false)
scalaVersion := (if (buildType=="3") "2.12.16" else "2.11.12")

javacOptions ++= Seq("-source", "8", "-target", "8")

val spark2Version = "2.4.0"
val spark3Version = "3.3.0"
val sparkVersion = (if (buildType=="3") spark3Version else spark2Version)

lazy val configString = settingKey[String]("dump").withRank(KeyRanks.Invisible)
configString := s"using scala: ${scalaVersion.value} and spark: $sparkVersion [standalone=$standalone]"

val sparkDeps = Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-avro" % sparkVersion,
  "org.apache.spark" %% "spark-hive-thriftserver" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-hive-thriftserver" % sparkVersion,
  "org.apache.spark" %% "spark-unsafe" % sparkVersion,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion,
)

val commonDeps = Seq(
  "io.circe" %% "circe-config" % (if (sparkVersion==spark2Version) "0.7.0-M1" else "0.8.0"),
  "io.circe" %% "circe-yaml" % (if (sparkVersion==spark2Version) "0.7.0-M1" else "0.8.0"),
  "io.circe" %% "circe-core" % (if (sparkVersion==spark2Version) "0.7.0-M1" else "0.8.0"),
  "io.circe" %% "circe-parser" % (if (sparkVersion==spark2Version) "0.7.0-M1" else "0.8.0"),
  "io.circe" %% "circe-generic" % (if (sparkVersion==spark2Version) "0.7.0-M1" else "0.8.0"),
  "org.freemarker" % "freemarker" % "2.3.31",
  "org.rogach" %% "scallop" % "4.1.0",
  "org.jline" % "jline" % "3.0.0.M2",
  "com.github.javafaker" % "javafaker" % "1.0.2",
  "org.apache.hbase" % "hbase-common" % "2.2.0" % "provided" intransitive,
  "org.apache.hbase" % "hbase-client" % "2.2.0" % "provided" intransitive,
  "com.amazon.deequ" % "deequ" % (if (sparkVersion==spark2Version) "1.2.2-spark-2.4" else "1.2.2-spark-3.0")
)

Compile / unmanagedSourceDirectories += {
  baseDirectory.value / s"src/main/scala_${scalaVersion.value.substring(0,4)}"
}

//resolvers += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
libraryDependencies ++= (if (standalone) sparkDeps else sparkDeps.map(dep => dep % Provided))
libraryDependencies ++= commonDeps
libraryDependencies += "junit" % "junit" % "4.13.2" % Test

assembly / mainClass := Some("com.github.supermariolabs.spooq.Application")
assemblyJarName := s"${name.value}-${version.value}-spark${sparkVersion}_${scalaVersion.value}${if (standalone) "-standalone" else ""}.jar"

Runtime / fullClasspathAsJars := (Test / fullClasspathAsJars).value

assemblyMergeStrategy := {
  case PathList("META-INF","services", xs @ _*) => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.last
}

Compile/compile := (Compile/compile dependsOn nextBuild).value

lazy val nextBuild = taskKey[Unit]("Increment the build number")
nextBuild := {
  val lastbuild = Source.fromFile("src/main/resources/lastbuild").getLines.mkString.toLong
  println(s"actual build: ${lastbuild}, new build: ${lastbuild+1}")
  Some(new PrintWriter("src/main/resources/lastbuild")).foreach{p => p.write(s"${lastbuild+1}"); p.close}
}