package com.gnmathur.wingspan.refapplications

import com.gnmathur.wingspan.core.{Reactor, ReactorConnectionCtx, TimerCb}

import java.nio.channels.SocketChannel

class TimedFramedEchoClient(coreReactor: Reactor) extends FramedEchoClient(coreReactor) {
  override def readDoneCb(sc: SocketChannel, reactorConnectionContext: ReactorConnectionCtx, clientMetadata: AnyRef): Unit = {
    val cm = clientMetadata.asInstanceOf[ConnectionContext]
    cm.readState match {
      case READ_NEW =>
        logger.info("read done")
        logger.info("read: " + new String(cm.readBytes.get))
        coreReactor.clearRead(reactorConnectionContext)

        val randomJitter = scala.util.Random.nextInt(1000)
        val addOrSub = if (scala.util.Random.nextInt(100) < 50) -1 else 1

        //coreReactor.registerTimer(TimerCb(5000 + (addOrSub * randomJitter), false, () => {
        //  coreReactor.setWriteReady(reactorConnectionContext)
        //}))
      case READ_LEN =>
        coreReactor.setReadReady(reactorConnectionContext)
    }
  }
}

object TimedFramedEchoClient extends App {
  private val coreReactor: Reactor = new Reactor()
  new TimedFramedEchoClient(coreReactor)
  coreReactor.run()
}
