package nl.vroste.zio.kinesis.client

import java.util.UUID

import nl.vroste.zio.kinesis.client.DynamicConsumer.{ Checkpointer, Record }
import nl.vroste.zio.kinesis.client.serde.Deserializer
import software.amazon.kinesis.common.{ InitialPositionInStream, InitialPositionInStreamExtended }
import zio.blocking.Blocking
import zio.stream.ZStream
import zio.{ Has, UIO }

object DynamicConsumer2 {
  type DynamicConsumer2 = Has[Service]

  trait Service {
    def shardedStream[R, T](
      streamName: String,
      applicationName: String,
      deserializer: Deserializer[R, T], // TODO make Deserializer a ZLayer too
      requestShutdown: UIO[Unit] = UIO.never,
      initialPosition: InitialPositionInStreamExtended =
        InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
      isEnhancedFanOut: Boolean = true,
      leaseTableName: Option[String] = None,
      workerIdentifier: String = UUID.randomUUID().toString,
      maxShardBufferSize: Int = 1024    // Prefer powers of 2
    ): ZStream[
      Blocking with R,
      Throwable,
      (String, ZStream[Blocking, Throwable, Record[T]], Checkpointer)
    ]                                   // TODO - what about Blocking?
  }
}
