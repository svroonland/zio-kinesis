package nl.vroste.zio.kinesis

import zio.Has

package object client {

  type AdminClient     = Has[AdminClient.Service]
  type Client          = Has[Client.Service]
  type DynamicConsumer = Has[DynamicConsumer.Service]
}
