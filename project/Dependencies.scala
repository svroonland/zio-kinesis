import sbt._

object Dependencies {
  object Versions {
    val hdrHistogram              = "2.2.2"
    val jaxB                      = "2.3.1"
    val kcl                       = "3.3.0"
    val zio                       = "2.1.24"
    val zioAws                    = "7.41.34.2"
    val zioInteropReactiveStreams = "2.0.2"
    val zioLogging                = "2.5.3"
  }

  import Versions._

  val Common =
    List(
      "dev.zio"         %% "zio"                         % zio,
      "dev.zio"         %% "zio-aws-core"                % zioAws,
      "dev.zio"         %% "zio-aws-kinesis"             % zioAws,
      "dev.zio"         %% "zio-aws-dynamodb"            % zioAws,
      "dev.zio"         %% "zio-aws-cloudwatch"          % zioAws,
      "dev.zio"         %% "zio-aws-netty"               % zioAws,
      "dev.zio"         %% "zio-streams"                 % zio,
      "dev.zio"         %% "zio-interop-reactivestreams" % zioInteropReactiveStreams,
      "dev.zio"         %% "zio-logging"                 % zioLogging,
      "dev.zio"         %% "zio-logging-slf4j"           % zioLogging,
      "javax.xml.bind"   % "jaxb-api"                    % jaxB,
      "org.hdrhistogram" % "HdrHistogram"                % hdrHistogram,
      "dev.zio"         %% "zio-test"                    % zio % Test,
      "dev.zio"         %% "zio-test-sbt"                % zio % Test
    )

  val DynamicConsumer =
    List(
      "software.amazon.kinesis" % "amazon-kinesis-client" % kcl
    )
}
