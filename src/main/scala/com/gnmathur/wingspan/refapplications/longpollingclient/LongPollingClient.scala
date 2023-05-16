package com.gnmathur.wingspan.refapplications.longpollingclient

import com.gnmathur.wingspan.core._
import com.gnmathur.wingspan.refapplications.periodicechoclient.PeriodicEchoClient

object LongPollingClient extends App {
  private val coreReactor: Reactor = new Reactor()
  private val lpc = new LongPollingClient(coreReactor)

  coreReactor.registerClient(lpc)

  lpc.runClient(LongPoll, "sys76-1", 6770, "Tis better to have loved and lost than never to have loved at all")
  lpc.runClient(LongPoll, "sys76-1", 6771, "I think therefore I am")

  coreReactor.run()
}

class LongPollingClient(coreReactor: Reactor) extends PeriodicEchoClient(coreReactor) {
}