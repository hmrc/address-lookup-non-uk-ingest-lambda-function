ThisBuild / name := "address-lookup-non-uk-ingest-lambda-functions"
ThisBuild / version := "1.1"
ThisBuild / scalaVersion := "2.12.12"
ThisBuild / assemblyJarName := "address-lookup-non-uk-ingest-lambda-functions_2.12-1.1.jar"
ThisBuild / parallelExecution := false

val doobieVersion = "0.7.1"

ThisBuild / libraryDependencies ++= Seq(
  "com.amazonaws"        % "aws-lambda-java-core" % "1.2.1",
  "com.amazonaws"        % "aws-java-sdk-s3"      % "1.11.921",
  "me.lamouri"           % "jcredstash"           % "2.1.1",
  "com.lihaoyi"         %% "os-lib"               % "0.7.1",
  "org.tpolecat"        %% "doobie-core"          % doobieVersion,
  "org.tpolecat"        %% "doobie-postgres"      % doobieVersion
)
