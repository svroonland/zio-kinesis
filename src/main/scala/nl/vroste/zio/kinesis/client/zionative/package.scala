package nl.vroste.zio.kinesis.client
import zio.Has

package object zionative {
  type LeaseRepository        = Has[LeaseRepository.Service]
  type LeaseRepositoryFactory = Has[LeaseRepository.Factory]

}
