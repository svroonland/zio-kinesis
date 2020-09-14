package nl.vroste.zio.kinesis.client.examples.app.adhoc

import zio.Has

package object services {

  type Config         = Has[Config.Service]
  type EndpointClient = Has[EndpointClient.Service]
  type Repository     = Has[Repository.Service]
}
