lazy val commonSettings = Seq(
  name := "idus-task",
  version := "0.1",
  scalaVersion := "2.13.7"
)

lazy val app = (project in file(".")).
  settings(commonSettings: _*)

libraryDependencies ++= Seq(
  // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
  "org.apache.spark" %% "spark-sql" % "3.2.0" % "provided",
  // https://mvnrepository.com/artifact/log4j/log4j
  "log4j" % "log4j" % "1.2.17"

)

mainClass in assembly := Some("idus.DataEngineerTask")
assemblyJarName := "idus.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}