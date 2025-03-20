import xerial.sbt.Sonatype.GitHubHosting
import org.typelevel.scalacoptions.ScalacOptions

val mainScala = "2.13.16"
val allScala  = Seq(mainScala, "3.3.5")

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
    resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    // Workaround for "/Users/steven/projects/personal/zio/zio-kinesis/core/src/main/scala/nl/vroste/zio/kinesis/client/zionative/Consumer.scala:317:17: pattern var shardStream in value $anonfun is never used"
    tpolecatExcludeOptions += ScalacOptions.warnUnusedPatVars
  )
)

val zioVersion    = "2.1.14"
val zioAwsVersion = "7.31.1.5"

lazy val root = project
  .in(file("."))
  .settings(
    Seq(
      scalafmtOnCompile := false
    )
  )
  .settings(stdSettings: _*)
  .aggregate(core, interopFutures, dynamicConsumer, tests, testUtils)
  .dependsOn(core, interopFutures, dynamicConsumer, tests, testUtils)

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
  Compile / scalacOptions ~= excludeInferAny,
  // Suppresses problems with Scaladoc @throws links
  testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
  libraryDependencies ++= Seq(
    "dev.zio"         %% "zio"                         % zioVersion,
    "dev.zio"         %% "zio-streams"                 % zioVersion,
    "dev.zio"         %% "zio-test"                    % zioVersion % "test",
    "dev.zio"         %% "zio-test-sbt"                % zioVersion % "test",
    "dev.zio"         %% "zio-interop-reactivestreams" % "2.0.2",
    "dev.zio"         %% "zio-logging"                 % "2.5.0",
    "dev.zio"         %% "zio-logging-slf4j"           % "2.5.0",
    "ch.qos.logback"   % "logback-classic"             % "1.5.17",
    "org.hdrhistogram" % "HdrHistogram"                % "2.2.2",
    "dev.zio"         %% "zio-aws-core"                % zioAwsVersion,
    "dev.zio"         %% "zio-aws-kinesis"             % zioAwsVersion,
    "dev.zio"         %% "zio-aws-dynamodb"            % zioAwsVersion,
    "dev.zio"         %% "zio-aws-cloudwatch"          % zioAwsVersion,
    "dev.zio"         %% "zio-aws-netty"               % zioAwsVersion,
    "javax.xml.bind"   % "jaxb-api"                    % "2.3.1"
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
      "software.amazon.kinesis" % "amazon-kinesis-client" % "3.0.2"
    )
  )
  .dependsOn(core % "compile->compile;test->test")

lazy val testUtils = (project in file("test-utils"))
  .settings(stdSettings: _*)
  .settings(
    name                       := "zio-kinesis-test-utils",
    assembly / assemblyJarName := "zio-kinesis-test-utils" + version.value + ".jar"
  )
  .dependsOn(dynamicConsumer % "compile->compile;test->test")

lazy val tests = (project in file("test"))
  .dependsOn(dynamicConsumer % "compile->compile;test->test", testUtils % "compile->compile")
  .settings(stdSettings: _*)
  .settings(publish / skip := true)
