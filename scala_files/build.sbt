import Dependencies._

ThisBuild / scalaVersion     := "2.12.8"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "Scala_Files",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test,
    libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.8.0",
    libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.0",
    libraryDependencies += "com.typesafe" % "config" % "1.2.1",
    libraryDependencies += "org.scalaj" % "scalaj-http_2.11" % "2.3.0",
    libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.2.1",
    libraryDependencies += "org.apache.hive" % "hive-jdbc" % "2.3.5",
    libraryDependencies += "io.github.zamblauskas" %% "scala-csv-parser" % "0.13.1",
    libraryDependencies += "com.lihaoyi" %% "upickle" % "0.9.5",
    libraryDependencies += "com.lihaoyi" %% "os-lib" % "0.7.8",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3",
    libraryDependencies += "com.typesafe.play" %% "play-json" % "2.7.0"
  )

// Uncomment the following for publishing to Sonatype.
// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for more detail.

// ThisBuild / description := "Some descripiton about your project."
// ThisBuild / licenses    := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
// ThisBuild / homepage    := Some(url("https://github.com/example/project"))
// ThisBuild / scmInfo := Some(
//   ScmInfo(
//     url("https://github.com/your-account/your-project"),
//     "scm:git@github.com:your-account/your-project.git"
//   )
// )
// ThisBuild / developers := List(
//   Developer(
//     id    = "Your identifier",
//     name  = "Your Name",
//     email = "your@email",
//     url   = url("http://your.url")
//   )
// )
// ThisBuild / pomIncludeRepository := { _ => false }
// ThisBuild / publishTo := {
//   val nexus = "https://oss.sonatype.org/"
//   if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
//   else Some("releases" at nexus + "service/local/staging/deploy/maven2")
// }
// ThisBuild / publishMavenStyle := true
