package nl.vroste.zio.kinesis.client.examples.app.adhoc.model

case class ConfigData(server: Server = Server(), kinesis: Kinesis = Kinesis())
case class Server(host: String = "localhost", port: Int = 8000)
case class Kinesis(
  streamName: String = "mercury-invoice-generator-adhoc-stream-dev",
  appName: String = "mercury-invoice-generator-adhoc-app-dev",
  shardCount: Int = 4,
  totalRecords: Int = 5000,
  batchSize: Int = 500
)
