package nl.vroste.zio.kinesis.client
import zio.stream.ZStream.Pull
import zio.stream.{ Take, ZStream, ZTransducer }
import zio._

object ZioFixes {

  implicit class MyZStreamExtensions[-R, +E, +O](self: ZStream[R, E, O]) {
    final def myAggregateAsyncWithin[R1 <: R, E1 >: E, P](
      transducer: ZTransducer[R1, E1, O, P],
      schedule: Schedule[R1, Chunk[P], Any]
    ): ZStream[R1, E1, P] =
      myAggregateAsyncWithinEither(transducer, schedule).collect {
        case Right(v) => v
      }

    // From PR #3751
    final def myAggregateAsyncWithinEither[R1 <: R, E1 >: E, P, Q](
      transducer: ZTransducer[R1, E1, O, P],
      schedule: Schedule[R1, Chunk[P], Q]
    ): ZStream[R1, E1, Either[Q, P]] =
      ZStream {
        for {
          pull          <- self.process
          push          <- transducer.push
          handoff       <- Handoff.make[Take[E1, O]].toManaged_
          raceNextTime  <- ZRef.makeManaged(false)
          waitingFiber  <- ZRef
                            .makeManaged[Option[Fiber[Nothing, Take[E1, O]]]](None)
          scheduleState <- schedule.initial
                             .flatMap(i => ZRef.make[(Chunk[P], schedule.State)](Chunk.empty -> i))
                             .toManaged_
          producer       = Take.fromPull(pull).doWhileM(take => handoff.offer(take).as(!take.isDone))
          consumer       = {
            // Advances the state of the schedule, which may or may not terminate
            val updateSchedule: URIO[R1, Option[schedule.State]] =
              scheduleState.get.flatMap(state => schedule.update(state._1, state._2).option)

            // Waiting for the normal output of the producer
            val waitForProducer: ZIO[R1, Nothing, Take[E1, O]] =
              waitingFiber.getAndSet(None).flatMap {
                case None      => handoff.take
                case Some(fib) => fib.join
              }

            def updateLastChunk(take: Take[_, P]): UIO[Unit] =
              take.tap(chunk => scheduleState.update(_.copy(_1 = chunk)))

            def handleTake(take: Take[E1, O]): Pull[R1, E1, Take[E1, Either[Nothing, P]]] =
              take
                .foldM(
                  push(None).map(ps => Chunk(Take.chunk(ps.map(Right(_))), Take.end)),
                  cause => ZIO.halt(cause),
                  os =>
                    Take
                      .fromPull(push(Some(os)).asSomeError)
                      .flatMap(take => updateLastChunk(take).as(Chunk.single(take.map(Right(_)))))
                )
                .mapError(Some(_))

            def go(race: Boolean): ZIO[R1, Option[E1], Chunk[Take[E1, Either[Q, P]]]] =
              if (!race)
                waitForProducer.flatMap(handleTake) <* raceNextTime.set(true)
              else
                updateSchedule.raceWith[R1, Nothing, Option[E1], Take[E1, O], Chunk[Take[E1, Either[Q, P]]]](
                  waitForProducer
                )(
                  (scheduleDone, producerWaiting) =>
                    ZIO.done(scheduleDone).flatMap {
                      case None            =>
                        for {
                          init          <- schedule.initial
                          state         <- scheduleState.getAndSet(Chunk.empty -> init)
                          scheduleResult = Take.single(Left(schedule.extract(state._1, state._2)))
                          take          <- Take.fromPull(push(None).asSomeError).tap(updateLastChunk)
                          _             <- raceNextTime.set(false)
                          _             <- producerWaiting.disown // To avoid interruption when this fiber is joined
                          _             <- waitingFiber.set(Some(producerWaiting))
                        } yield Chunk(scheduleResult, take.map(Right(_)))
                      case Some(nextState) =>
                        for {
                          _  <- scheduleState.update(_.copy(_2 = nextState))
                          ps <- Take.fromPull(push(None).asSomeError).tap(updateLastChunk)
                          _  <- raceNextTime.set(false)
                          _  <- producerWaiting.disown // To avoid interruption when this fiber is joined
                          _  <- waitingFiber.set(Some(producerWaiting))
                        } yield Chunk.single(ps.map(Right(_)))
                    },
                  (producerDone, scheduleWaiting) =>
                    scheduleWaiting.interrupt *> handleTake(Take(producerDone.flatMap(_.exit)))
                )

            raceNextTime.get
              .flatMap(go)
              .onInterrupt(waitingFiber.get.flatMap(_.map(_.interrupt).getOrElse(ZIO.unit)))

          }

          _             <- producer.forkManaged
        } yield consumer
      }.flattenTake

  }

  object Pull {
    def emit[A](a: A): IO[Nothing, Chunk[A]]                                      = UIO(Chunk.single(a))
    def emit[A](as: Chunk[A]): IO[Nothing, Chunk[A]]                              = UIO(as)
    def fromDequeue[E, A](d: Dequeue[stream.Take[E, A]]): IO[Option[E], Chunk[A]] = d.take.flatMap(_.done)
    def fail[E](e: E): IO[Option[E], Nothing]                                     = IO.fail(Some(e))
    def halt[E](c: Cause[E]): IO[Option[E], Nothing]                              = IO.halt(c).mapError(Some(_))
    def empty[A]: IO[Nothing, Chunk[A]]                                           = UIO(Chunk.empty)
    val end: IO[Option[Nothing], Nothing]                                         = IO.fail(None)
  }

  /**
   * A synchronous queue-like abstraction that allows a producer to offer
   * an element and wait for it to be taken, and allows a consumer to wait
   * for an element to be available.
   */
  class Handoff[A](ref: Ref[Handoff.State[A]]) {
    def offer(a: A): UIO[Unit] =
      Promise.make[Nothing, Unit].flatMap { p =>
        ref.modify {
          case s @ Handoff.State.Full(_, notifyProducer) => (notifyProducer.await *> offer(a), s)
          case Handoff.State.Empty(notifyConsumer)       => (notifyConsumer.succeed(()) *> p.await, Handoff.State.Full(a, p))
        }.flatten
      }

    def take: UIO[A] =
      Promise.make[Nothing, Unit].flatMap { p =>
        ref.modify {
          case Handoff.State.Full(a, notifyProducer)   => (notifyProducer.succeed(()).as(a), Handoff.State.Empty(p))
          case s @ Handoff.State.Empty(notifyConsumer) => (notifyConsumer.await *> take, s)
        }.flatten
      }

    def poll: UIO[Option[A]] =
      Promise.make[Nothing, Unit].flatMap { p =>
        ref.modify {
          case Handoff.State.Full(a, notifyProducer) => (notifyProducer.succeed(()).as(Some(a)), Handoff.State.Empty(p))
          //          case s @ Handoff.State.Empty(_)            => (ZIO.succeedNow(None), s)
          case s @ Handoff.State.Empty(_)            => (ZIO.succeed(None), s)
        }.flatten
      }
  }

  object Handoff {
    def make[A]: UIO[Handoff[A]] =
      Promise
        .make[Nothing, Unit]
        .flatMap(p => Ref.make[State[A]](State.Empty(p)))
        .map(new Handoff(_))

    sealed trait State[+A]
    object State {
      case class Empty(notifyConsumer: Promise[Nothing, Unit])          extends State[Nothing]
      case class Full[+A](a: A, notifyProducer: Promise[Nothing, Unit]) extends State[A]
    }
  }
}
