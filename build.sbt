val mainScala = "2.12.10"
val allScala  = Seq("2.11.12", mainScala)

inThisBuild(
  List(
    organization := "nl.vroste",
    homepage := Some(url("https://github.com/svroonland/zio-kinesis")),
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    useCoursier := false,
    scalaVersion := mainScala,
    crossScalaVersions := allScala,
    parallelExecution in Test := false,
    fork in Test := true,
    fork in run := true
  )
)

ThisBuild / publishTo := sonatypePublishToBundle.value

name := "zio-kinesis"
scalafmtOnCompile := true

enablePlugins(BuildInfoPlugin)
buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion, isSnapshot)
buildInfoPackage := "zio.kinesis"
buildInfoObject := "BuildInfo"

libraryDependencies ++= Seq(
  "dev.zio"                 %% "zio-streams"                 % "1.0.0-RC16",
  "dev.zio"                 %% "zio-test"                    % "1.0.0-RC16" % "test",
  "dev.zio"                 %% "zio-test-sbt"                % "1.0.0-RC16" % "test",
  "dev.zio"                 %% "zio-interop-java"            % "1.1.0.0-RC5",
  "dev.zio"                 %% "zio-interop-reactivestreams" % "1.0.3.4-RC1",
  "software.amazon.awssdk"  % "kinesis"                      % "2.10.3",
  "software.amazon.kinesis" % "amazon-kinesis-client"        % "2.2.5",
  "org.scalatest"           %% "scalatest"                   % "3.0.5" % "test",
  "ch.qos.logback"          % "logback-classic"              % "1.2.3",
//  "org.slf4j"               % "jul-to-slf4j"                 % "1.7.28",
  compilerPlugin("org.spire-math" %% "kind-projector" % "0.9.10")
)

testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")
