import xerial.sbt.Sonatype.GitHubHosting

val mainScala = "2.13.10"
val allScala  = Seq("2.12.17", mainScala, "3.2.2")

val excludeInferAny = { options: Seq[String] => options.filterNot(Set("-Xlint:infer-any")) }

inThisBuild(
  List(
    organization                     := "nl.vroste",
    homepage                         := Some(url("https://github.com/svroonland/zio-kinesis")),
    licenses                         := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    scalaVersion                     := mainScala,
    crossScalaVersions               := allScala,
    compileOrder                     := CompileOrder.JavaThenScala,
    Test / parallelExecution         := false,
    Global / cancelable              := true,
    Test / fork                      := true,
    Test / fork                      := true,
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*)       => MergeStrategy.discard
      case n if n.startsWith("reference.conf") => MergeStrategy.concat
      case _                                   => MergeStrategy.first
    },
    scmInfo                          := Some(
      ScmInfo(url("https://github.com/svroonland/zio-kinesis/"), "scm:git:git@github.com:svroonland/zio-kinesis.git")
    ),
    sonatypeProjectHosting           := Some(
      GitHubHosting("svroonland", "zio-kinesis", "info@vroste.nl")
    ),
    developers                       := List(
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

val zioVersion    = "2.0.10"
val zioAwsVersion = "5.20.22.3"

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
  Compile / compile / javacOptions ++= Seq("--release", "8"),
  Compile / compile / scalacOptions ++= {
    // This is for scala.collection.compat._
    if (scalaBinaryVersion.value == "2.13")
      Seq("-Wconf:cat=unused-imports:silent")
    else Seq.empty
  } ++ Seq("-release", "8"),
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
  Compile / scalacOptions ~= excludeInferAny,
  // Suppresses problems with Scaladoc @throws links
  testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
  libraryDependencies ++= Seq(
    "dev.zio"                %% "zio"                         % zioVersion,
    "dev.zio"                %% "zio-streams"                 % zioVersion,
    "dev.zio"                %% "zio-test"                    % zioVersion % "test",
    "dev.zio"                %% "zio-test-sbt"                % zioVersion % "test",
    "dev.zio"                %% "zio-interop-reactivestreams" % "2.0.0",
    "dev.zio"                %% "zio-logging"                 % "2.1.8",
    "dev.zio"                %% "zio-logging-slf4j"           % "2.1.8",
    "ch.qos.logback"          % "logback-classic"             % "1.4.5",
    "org.scala-lang.modules" %% "scala-collection-compat"     % "2.9.0",
    "org.hdrhistogram"        % "HdrHistogram"                % "2.1.12",
    "dev.zio"                %% "zio-aws-core"                % zioAwsVersion,
    "dev.zio"                %% "zio-aws-kinesis"             % zioAwsVersion,
    "dev.zio"                %% "zio-aws-dynamodb"            % zioAwsVersion,
    "dev.zio"                %% "zio-aws-cloudwatch"          % zioAwsVersion,
    "dev.zio"                %% "zio-aws-netty"               % zioAwsVersion,
    "javax.xml.bind"          % "jaxb-api"                    % "2.3.1"
  )
)

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

lazy val interopFutures = (project in file("interop-futures"))
  .settings(stdSettings: _*)
  .settings(
    name                       := "zio-kinesis-future",
    assembly / assemblyJarName := "zio-kinesis-future" + version.value + ".jar",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio-interop-reactivestreams" % "1.3.4"
    )
  )
  .dependsOn(core)

lazy val dynamicConsumer = (project in file("dynamic-consumer"))
  .settings(stdSettings: _*)
  .settings(
    name                       := "zio-kinesis-dynamic-consumer",
    assembly / assemblyJarName := "zio-kinesis-dynamic-consumer" + version.value + ".jar",
    libraryDependencies ++= Seq(
      "software.amazon.kinesis" % "amazon-kinesis-client" % "2.4.5"
    )
  )
  .dependsOn(core % "compile->compile;test->test")
