import xerial.sbt.Sonatype.GitHubHosting

val mainScala = "2.13.6"
val allScala  = Seq("2.12.15", mainScala)

inThisBuild(
  List(
    organization := "nl.vroste",
    homepage := Some(url("https://github.com/svroonland/zio-kinesis")),
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    scalaVersion := mainScala,
    crossScalaVersions := allScala,
    Test / parallelExecution := false,
    Global / cancelable := true,
    Test / fork := true,
    Test / fork := true,
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*)       => MergeStrategy.discard
      case n if n.startsWith("reference.conf") => MergeStrategy.concat
      case _                                   => MergeStrategy.first
    },
    scmInfo := Some(
      ScmInfo(url("https://github.com/svroonland/zio-kinesis/"), "scm:git:git@github.com:svroonland/zio-kinesis.git")
    ),
    sonatypeProjectHosting := Some(
      GitHubHosting("svroonland", "zio-kinesis", "info@vroste.nl")
    ),
    developers := List(
      Developer(
        "svroonland",
        "Vroste",
        "info@vroste.nl",
        url("https://github.com/svroonland")
      )
    ),
    resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
  )
)

val zioVersion    = "1.0.12"
val zioAwsVersion = "3.17.41.1"

lazy val root = project
  .in(file("."))
  .settings(
    Seq(
      scalafmtOnCompile := false
    )
  )
  .settings(stdSettings: _*)
  .aggregate(core, interopFutures, dynamicConsumer)
  .dependsOn(core, interopFutures, dynamicConsumer)

lazy val core = (project in file("core"))
  .enablePlugins(ProtobufPlugin)
  .settings(stdSettings: _*)
  .settings(
    Seq(
      name := "zio-kinesis"
    )
  )

lazy val stdSettings: Seq[sbt.Def.SettingsDefinition] = Seq(
  Compile / compile / scalacOptions ++= {
    // This is for scala.collection.compat._
    if (scalaBinaryVersion.value == "2.13")
      Seq("-Wconf:cat=unused-imports:silent")
    else Seq.empty
  },
  Test / compile / scalacOptions ++= {
    // This is for scala.collection.compat._
    if (scalaBinaryVersion.value == "2.13")
      Seq("-Wconf:cat=unused-imports:silent")
    else Seq.empty
  },
  Compile / doc / scalacOptions ++= {
    // This is for scala.collection.compat._
    if (scalaBinaryVersion.value == "2.13")
      Seq("-Wconf:cat=unused-imports:silent")
    else Seq.empty
  },
// Suppresses problems with Scaladoc @throws links
  testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
  libraryDependencies ++= Seq(
    "dev.zio"                %% "zio"                         % zioVersion,
    "dev.zio"                %% "zio-streams"                 % zioVersion,
    "dev.zio"                %% "zio-test"                    % zioVersion % "test",
    "dev.zio"                %% "zio-test-sbt"                % zioVersion % "test",
    "dev.zio"                %% "zio-interop-reactivestreams" % "1.3.7",
    "dev.zio"                %% "izumi-reflect"               % "2.0.1", // Temporarily to fix issue with Tag in zio 1.0.8
    "dev.zio"                %% "zio-logging"                 % "0.5.12",
    "ch.qos.logback"          % "logback-classic"             % "1.2.6",
    "org.scala-lang.modules" %% "scala-collection-compat"     % "2.5.0",
    "org.hdrhistogram"        % "HdrHistogram"                % "2.1.12",
    "io.github.vigoo"        %% "zio-aws-core"                % zioAwsVersion,
    "io.github.vigoo"        %% "zio-aws-kinesis"             % zioAwsVersion,
    "io.github.vigoo"        %% "zio-aws-dynamodb"            % zioAwsVersion,
    "io.github.vigoo"        %% "zio-aws-cloudwatch"          % zioAwsVersion,
    "io.github.vigoo"        %% "zio-aws-netty"               % zioAwsVersion,
    "javax.xml.bind"          % "jaxb-api"                    % "2.3.1"
  )
)

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

lazy val interopFutures = (project in file("interop-futures"))
  .settings(stdSettings: _*)
  .settings(
    name := "zio-kinesis-future",
    assembly / assemblyJarName := "zio-kinesis-future" + version.value + ".jar",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio-interop-reactivestreams" % "1.3.4"
    )
  )
  .dependsOn(core)

lazy val dynamicConsumer = (project in file("dynamic-consumer"))
  .settings(stdSettings: _*)
  .settings(
    name := "zio-kinesis-dynamic-consumer",
    assembly / assemblyJarName := "zio-kinesis-dynamic-consumer" + version.value + ".jar",
    libraryDependencies ++= Seq(
      "software.amazon.kinesis" % "amazon-kinesis-client" % "2.3.6"
    )
  )
  .dependsOn(core % "compile->compile;test->test")
