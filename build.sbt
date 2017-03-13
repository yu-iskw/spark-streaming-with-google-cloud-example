name := "spark-streaming-with-google-cloud-example"

version := "0.1"

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.10.6", "2.11.8")

sparkVersion := "2.0.1"

test in assembly := {}

// Can't parallelly execute in test
parallelExecution in Test := false

fork in Test := true

spAppendScalaVersion := true

spIncludeMaven := true

spIgnoreProvided := true

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.getOrElse("spark.testVersion", sparkVersion.value)

javaOptions ++= Seq("-Xmx2G", "-XX:MaxPermSize=256m")

libraryDependencies ++= Seq(
  "org.json4s" % "json4s-native_2.11" % "3.5.0",
  "org.hamcrest" % "hamcrest-all" % "1.3",
  "com.google.cloud" % "google-cloud-pubsub" % "0.8.3-alpha",
  "com.google.apis" % "google-api-services-pubsub" % "v1-rev7-1.20.0",
  "com.google.cloud" % "google-cloud-datastore" % "0.9.3-beta" excludeAll (
    ExclusionRule(organization = "com.fasterxml.jackson.core", name = "jackson-core")
  ),
  "org.apache.spark" %% "spark-core" % testSparkVersion.value % "provided",
  "org.apache.spark" %% "spark-yarn" % testSparkVersion.value % "provided",
  "org.apache.spark" %% "spark-mllib" % testSparkVersion.value % "provided",
  "org.apache.spark" %% "spark-streaming" % testSparkVersion.value % "provided",
  "org.apache.spark" %% "spark-sql" % testSparkVersion.value % "provided",
  "org.apache.spark" %% "spark-catalyst" % testSparkVersion.value % "provided",
  "org.apache.spark" %% "spark-hive" % testSparkVersion.value % "provided",
  "org.apache.spark" %% "spark-graphx" % testSparkVersion.value % "provided",
  "org.apache.spark" %% "spark-repl" % testSparkVersion.value % "provided",
  "com.google.protobuf" % "protobuf-java" % "3.0.0" force(),
  "com.github.scopt" %% "scopt" % "3.5.0",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "junit" % "junit" % "4.12"
)

resolvers ++= Seq(
  "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
  "Spray Repository" at "http://repo.spray.cc/",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
  "Atilika Open Source repository" at "http://www.atilika.org/nexus/content/repositories/atilika"
)

dependencyOverrides ++= Set(
  // jackson is conflict among Google's libraries and Spark's libraries
  "com.fasterxml.jackson.core" % "jackson-core" % "2.6.2",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.2"
)

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
  case "log4j.properties" => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}

// To resolve conflict among depended library versions.
assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.protobuf.*" -> "shadedproto.@1").inAll
)
