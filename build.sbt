ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.14"

val doobieVersion = "0.7.1"

lazy val root = (project in file("."))
  .settings(
    name := "address-lookup-international-addresses-ingest-lambda-function",
    libraryDependencies ++= Seq(
      "com.typesafe.play" %% "play-json" % "2.8.2",
      "com.amazonaws" % "aws-lambda-java-core" % "1.1.0",
      "me.lamouri" % "jcredstash" % "2.1.1",
      "com.lihaoyi" %% "os-lib" % "0.7.1",
      "ch.qos.logback" % "logback-core" % "1.2.3",
      "org.tpolecat" %% "doobie-core" % doobieVersion,
      "org.tpolecat" %% "doobie-hikari" % doobieVersion,
      "org.tpolecat" %% "doobie-postgres" % doobieVersion,
    )
  )
