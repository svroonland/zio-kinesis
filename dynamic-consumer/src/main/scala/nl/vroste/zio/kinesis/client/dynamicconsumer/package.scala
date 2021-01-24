package nl.vroste.zio.kinesis.client

import zio.Has

package object dynamicconsumer {
  type DynamicConsumer = Has[DynamicConsumer.Service]
}
