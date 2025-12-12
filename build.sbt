val appVersion = "2.0.0"

ThisBuild / name := "address-lookup-non-uk-ingest-lambda-functions"
ThisBuild / version := appVersion
ThisBuild / scalaVersion := "3.3.7"
ThisBuild / assemblyJarName := s"address-lookup-non-uk-ingest-lambda-functions_3-$appVersion.jar"
ThisBuild / parallelExecution := false

val doobieVersion = "1.0.0-RC11"

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
  case PathList("mime.types") => MergeStrategy.first
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}

ThisBuild / libraryDependencies ++= Seq(
  "com.amazonaws"           %   "aws-lambda-java-core"  % "1.4.0",
  "software.amazon.awssdk"  %   "s3"                    % "2.40.6",
  "me.lamouri"              %   "jcredstash"            % "2.1.1",
  "com.lihaoyi"             %%  "os-lib"                % "0.11.6",
  "org.tpolecat"            %%  "doobie-core"           % doobieVersion,
  "org.tpolecat"            %%  "doobie-postgres"       % doobieVersion
)
