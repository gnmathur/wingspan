/*
MIT License

Copyright (c) 2023 Gaurav Mathur

Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included in all
  copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  SOFTWARE.
*/

package com.gnmathur.wingspan.core

import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.exporter.HTTPServer.Builder
import org.slf4j.{Logger, LoggerFactory}

import java.io.IOException
import java.net.{InetAddress, InetSocketAddress}
import java.nio.channels.SelectionKey.{OP_READ, OP_WRITE}
import java.nio.channels.{SelectionKey, Selector, SocketChannel}
import java.util
import java.util.{Date, UUID}
import java.util.concurrent.{Semaphore, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable
import scala.util.{Try, Success, Failure}

private case class ClientConnectionStats() {
  var connectAttempts = 0L
  var connectFails = 0L
  var connectSuccess = 0L
  var reads = 0L
  var writes = 0L
  var readFails = 0L
  var writeFails = 0L
  var lastReqSentAt: Option[Long] = None
  var lastRespRcvdAt: Option[Long] = None
}

sealed abstract class Period
case class Periodic(periodInMs: Long) extends Period
case object LongPoll extends Period

sealed abstract class ConnectionState
case object Connecting extends ConnectionState
case object Connected extends ConnectionState
case object Disconnected extends ConnectionState

// Container for client's connection-specific context, including the client's metadata, which is opaque to the
// reactor
private case class ReactorClientConnCtx(
                                         clientMetadata: AnyRef,
                                         host: String,
                                         port: Int,
                                         periodInMs: Long
                                       ) {
  val stats = ClientConnectionStats()
  var state: ConnectionState = Disconnected
}

// Connection context understood by the reactor but opaque to the client. The client is supposed to return this back
// in specific reactor APIs
case class ReactorConnectionCtx(ctx: AnyRef)

trait ClientHandlers {

  /**
   * Client callback for when a connection is successfully established
   * @param sc The Socket channel associated with the connection
   * @param connectionContext Opaque state supplied by the reactor. Its supposed to be returned back to the reactor
   *                          in specific reactor APIs
   * @param clientMetadata Opaque data supplied by the client. It typically encapsulates the client state. Client
   *                       can use this when this callback is invoked to take stateful action
   */
  def connectDoneCb(sc: SocketChannel, connectionContext: ReactorConnectionCtx, clientMetadata: AnyRef): Unit

  /**
   * Client callback for when a connection fails to establish
   *
   * @param sc Socket channel associated with the connection attempt
   * @param clientMetadata Opaque data supplied by the client. It typically encapsulates the client state. Client
   * can use this when this callback is invoked to take stateful action
   */
  def connectFailCb(sc: SocketChannel, clientMetadata: AnyRef): Unit

  def disconnectCb(sc: SocketChannel, clientMetadata: AnyRef): Unit

  /**
   * Read callback for when there's something to be read from the a connection channel
   *
   * @param sc Socket channel associated with the client connection
   * @param clientMetadata Opaque data supplied by the client. It typically encapsulates the client state. Client
   * can use this when this callback is invoked to take stateful action
   * @return The client instructs what's the next socket operation to set on the selector via type NEXT_OP_T
   */
  def readCb(sc: SocketChannel, clientMetadata: AnyRef): EVENT_CB_STATUS_T

  /**
   * Read callback for when there's nothing more to be read from the a connection channel
   *
   * @param sc Socket channel associated with the client connection
   * @param connectionContext Opaque state supplied by the reactor. Its supposed to be returned back to the reactor in some APIs
   * @param clientMetadata Opaque data supplied by the client. It typically encapsulates the client state.
   */
  def readDoneCb(sc: SocketChannel, connectionContext: ReactorConnectionCtx, clientMetadata: AnyRef): Unit

  /**
   * Read callback for when there's an error reading from the a connection channel
   *
   * @param cc Socket channel associated with the client connection
   * @param connectionContext Opaque state supplied by the reactor. Its supposed to be returned back to the reactor in some APIs
   * @param clientMetadata Opaque data supplied by the client. It typically encapsulates the client state.
   */
  def readFailCb(cc: SocketChannel, connectionContext: ReactorConnectionCtx, clientMetadata: AnyRef): Unit

  /**
   * Write callback for when there's something to be written to the a connection channel
   * @param sc Socket channel associated with the client connection
   * @param clientMetadata Opaque data supplied by the client. It typically encapsulates the client state.
   * @return The client instructs what's the next socket operation to set on the selector via type NEXT_OP_T
   */
  def writeCb(sc: SocketChannel, clientMetadata: AnyRef): EVENT_CB_STATUS_T
  def writeDoneCb(sc: SocketChannel, connectionContext: ReactorConnectionCtx): Unit
  def writeFailCb(sc: SocketChannel, connectionContext: ReactorConnectionCtx, clientMetadata: AnyRef): Unit
}

object TimerIdGiver {
  private var _timerId = 0
  def give = {
    val r = _timerId
    _timerId = _timerId + 1
    r
  }
}

class TimerCb(timeout: Long,  // in milliseconds
              timerCb: => Unit // callback to invoke when the timer expires
             ) {
  val createdAt: Long = System.currentTimeMillis()
  val expiresAt: Long = createdAt + timeout
  def isExpired: Boolean = expiresAt < System.currentTimeMillis()
  def call(): Unit = timerCb
  def getTimeout: Long = timeout
  val id: Int = TimerIdGiver.give


  override def toString = s"TimerCb($createdAt, $expiresAt, $id, $timeout, $isExpired)"
}

class Reactor {
  private val selector: Selector = Selector.open()
  private def orderTimersByExpirationTime(timerCb: TimerCb) = timerCb.expiresAt
  private def orderTimersByPeriod(timerCb: TimerCb) = timerCb.getTimeout
  private val timers = new mutable.PriorityQueue[TimerCb]()(Ordering.by(orderTimersByExpirationTime).reverse)
  private val logger = LoggerFactory.getLogger(classOf[Reactor])
  private var clientHandlers: Option[ClientHandlers] = None
  private val clientConnectionList = mutable.LinkedHashMap[(String, Int), ReactorClientConnCtx]()
  private val keepRunning: AtomicBoolean = new AtomicBoolean(true)
  private val haltSignal = new Semaphore(0)
  private val registry = new CollectorRegistry()
  private val httpServer = new HTTPServer.Builder().withPort(8080).withPort(8081).build()
  private val SHUTDOWN_TIMER_EXPIRY_MS = 60000
  private val thisThread = Thread.currentThread()

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      logger.info("Shutting down reactor")
      keepRunning.set(false)

      if (haltSignal.tryAcquire(SHUTDOWN_TIMER_EXPIRY_MS, TimeUnit.MILLISECONDS)) {
        logger.info("Reactor shut down gracefully")
      } else {
        logger.error("Timed out waiting for reactor to shut down. Will interrupt the thread")
        thisThread.interrupt()
        if (haltSignal.tryAcquire(60, TimeUnit.MILLISECONDS)) {
          logger.info("Reactor shut down gracefully (after interrupt)")
        } else {
          logger.error("Timed out waiting for reactor to shut down. Will exit")
        }
      }
    }
  })

  private def stopReactor(): Unit = {
    logger.info("Closing metrics server")
    httpServer.close()
    logger.info("Closing selector")
    selector.close()
  }

  /**
   * Cancel the key associated with the channel and close it
   * @param cc Socket channel to close
   */
  private def closeConnection(cc: SocketChannel): Unit = {
    val selectionKey = cc.keyFor(selector)
    if (selectionKey.isValid)
      selectionKey.cancel()
    else
      logger.warn("Selection key is not valid. Ignoring")
    cc.close()
  }

  private def connect(reactorClientConnCtx: ReactorClientConnCtx): Unit = {
    if (reactorClientConnCtx.state != Disconnected) {
      logger.warn(s"Client ${reactorClientConnCtx.host}:${reactorClientConnCtx.port} not disconnected. Rescheduling for later")
      scheduleConnect(reactorClientConnCtx)
      return
    }

    val client: SocketChannel = SocketChannel.open();
    client.configureBlocking(false)
    val selectionKey: SelectionKey = client.register(selector, SelectionKey.OP_CONNECT);

    selectionKey.attach(reactorClientConnCtx)
    client.connect(new InetSocketAddress(reactorClientConnCtx.host, reactorClientConnCtx.port))
    reactorClientConnCtx.state = Connecting
    reactorClientConnCtx.stats.connectAttempts = reactorClientConnCtx.stats.connectAttempts + 1

    scheduleConnect(reactorClientConnCtx)
  }

  private def scheduleConnect(reactorClientConnCtx: ReactorClientConnCtx): Unit = {
    val randomJitter = scala.util.Random.nextInt(1000)
    val addOrSub = if (scala.util.Random.nextInt(100) < 50) -1 else 1

    registerTimer(new TimerCb(reactorClientConnCtx.periodInMs + (addOrSub * randomJitter), connect(reactorClientConnCtx)))
  }

  /**
   * Register a client connection endpoint and context. This routine will also initiate the connection
   *
   * @param host Connection endpoint host
   * @param port Connection endpoint host port
   * @param connectionCtx Opaque connection context to be returned back to client handlers callbacks
   */
  def registerRequest(host: String, port: Int, periodInMs: Long, connectionCtx: AnyRef): Unit = {
    val reactorClientConnCtx = ReactorClientConnCtx(connectionCtx, host, port, periodInMs)
    clientConnectionList += (host, port) -> reactorClientConnCtx

    scheduleConnect(reactorClientConnCtx)
  }

  /**
   *
   * @param key
   * @param clientSocket
   */
  private def handleReadable(key: SelectionKey, clientSocket: SocketChannel): Unit = {
    val reactorClientConnCtx = key.attachment().asInstanceOf[ReactorClientConnCtx]

    val r = clientHandlers.get.readCb(clientSocket, reactorClientConnCtx.clientMetadata)
    r match {
      case READ_OK =>
        require(clientSocket.isConnected)
        reactorClientConnCtx.stats.reads = reactorClientConnCtx.stats.reads + 1
        reactorClientConnCtx.stats.lastRespRcvdAt = Some(System.currentTimeMillis())
        clientHandlers.get.readDoneCb(clientSocket, ReactorConnectionCtx(key), reactorClientConnCtx.clientMetadata)

      case READ_ERROR =>
        reactorClientConnCtx.stats.readFails = reactorClientConnCtx.stats.readFails + 1
        clientHandlers.get.readFailCb(clientSocket, ReactorConnectionCtx(key), reactorClientConnCtx.clientMetadata)
        done(ReactorConnectionCtx(key))
    }
  }

  private def handleWritable(key: SelectionKey, clientSocket: SocketChannel): Unit = {
    val reactorClientConnCtx = key.attachment().asInstanceOf[ReactorClientConnCtx]

    val r = clientHandlers.get.writeCb(clientSocket, reactorClientConnCtx.clientMetadata)

    r match {
      case WRITE_OK =>
        require(clientSocket.isConnected)
        reactorClientConnCtx.stats.writes = reactorClientConnCtx.stats.writes + 1
        reactorClientConnCtx.stats.lastReqSentAt = Some(System.currentTimeMillis())
        clientHandlers.get.writeDoneCb(clientSocket, ReactorConnectionCtx(key))
      case WRITE_ERROR =>
        reactorClientConnCtx.stats.writeFails = reactorClientConnCtx.stats.writeFails + 1
        clientHandlers.get.writeFailCb(clientSocket, ReactorConnectionCtx(key), reactorClientConnCtx.clientMetadata)
    }
  }

  private def handleConnect(key: SelectionKey): Unit = {
    // Connected to server
    val client: SocketChannel = key.channel().asInstanceOf[SocketChannel]
    val reactorClientConnCtx = key.attachment().asInstanceOf[ReactorClientConnCtx]

    try {
      val isConnected = client.finishConnect()
      reactorClientConnCtx.state = Connected
      logger.debug("Connected: " + client.getRemoteAddress)
      reactorClientConnCtx.stats.connectSuccess = reactorClientConnCtx.stats.connectSuccess + 1
      key.interestOps(key.interestOps & (~SelectionKey.OP_CONNECT))
      clientHandlers.get.connectDoneCb(client, ReactorConnectionCtx(key), reactorClientConnCtx.clientMetadata)
    } catch {
      case e: IOException =>
        clientHandlers.get.connectFailCb(client, reactorClientConnCtx.clientMetadata)
        reactorClientConnCtx.stats.connectFails = reactorClientConnCtx.stats.connectFails + 1
        done(ReactorConnectionCtx(key))
      case _: Throwable => logger.error("Non I/O exception")
    }
  }

  private def statsPrinter(): Unit = {
    def printConnectionStats(host: String, port: Int, c: ClientConnectionStats): Unit = {
      val lastRequestSentAt = if (c.lastReqSentAt.isEmpty) s"<None>" else (new Date(c.lastReqSentAt.get)).toString
      val lastResponseRcvdAt = if (c.lastRespRcvdAt.isEmpty) s"<None>" else (new Date(c.lastRespRcvdAt.get)).toString

      val line =
        s"[host: ${host}, port: ${port}] TimerQ: ${timers.size} " +
          s"connAttempts: ${c.connectAttempts}, connFails: ${c.connectFails} connSucc: ${c.connectSuccess} " +
          s"read: ${c.reads} read: ${c.readFails} writes: ${c.writes} writeFails: ${c.writeFails} " +
          s"lastReqAt: ${lastResponseRcvdAt} lastRespAt: ${lastResponseRcvdAt}"

      logger.info(line)
    }

    clientConnectionList.foreach { case ((h, p), rCCCtx) => printConnectionStats(h, p, rCCCtx.stats) }
    registerTimer(new TimerCb(30000, statsPrinter))
  }

  def getConnectionRemoteHostName(cc: SocketChannel): String =
    Try {
      cc.getRemoteAddress.asInstanceOf[InetSocketAddress].getHostName
    } match {
      case Success(hostName) => hostName
      case Failure(exception) => "Unknown"
    }

  def getConnectionRemoteHostAddress(cc: SocketChannel): String =
    Try {
      cc.getRemoteAddress.asInstanceOf[InetSocketAddress].getAddress.getHostAddress
    } match {
      case Success(hostAddress) => hostAddress
      case Failure(exception) => "Unknown"
    }

  def getConnectionLocalEndpoint(cc: SocketChannel): String =
    Try {
      s"${cc.getLocalAddress.asInstanceOf[InetSocketAddress].getAddress}:${cc.getLocalAddress.asInstanceOf[InetSocketAddress].getPort}"
    } match {
      case Success(localEndpoint) => localEndpoint
      case Failure(exception) => "Unknown"
    }

  def getConnectionRemotePort(cc: SocketChannel): Try[Int] =
    Try(cc.getRemoteAddress.asInstanceOf[InetSocketAddress].getPort)

  def registerTimer(timerCb: TimerCb): Unit = {
    timers.enqueue(timerCb)
  }

  def registerClient(ch: ClientHandlers): Unit = {
    clientHandlers = Some(ch)
  }

  /**
   * Tell the reactor that the connection is ready for writing.
   *
   * @param rRef An opaque reference to the client, that was passed to the write callback
   * @return
   */
  def setWriteReady(rRef: ReactorConnectionCtx): Unit = {
    val key = rRef.ctx.asInstanceOf[SelectionKey]
    key.interestOps(OP_WRITE)
  }

  /**
   * Tell the reactor that the connection is ready for reading.
   * @param rRef An opaque reference to the client, that was passed to the read callback
   */
  def setReadReady(rRef: ReactorConnectionCtx): Unit = {
    val key = rRef.ctx.asInstanceOf[SelectionKey]
    key.interestOps(OP_READ)
  }

  def clearAll(rRef: ReactorConnectionCtx): Unit = {
    val key = rRef.ctx.asInstanceOf[SelectionKey]
    key.interestOps(~key.interestOps())
  }

  /**
   *  Tell the reactor that the connection is no longer interested in reading. This is necessary to do it explicitly in
   *  case of an invocation of the read callback that does not read anything from the socket. Otherwise, the reactor will
   *  keep on invoking the read callback.
   *
   * @param rRef A reference to the connection context that was passed to the read callback
   * @return None
   */
  def clearRead(rRef: ReactorConnectionCtx): Unit = {
    val key = rRef.ctx.asInstanceOf[SelectionKey]
    key.interestOps(key.interestOps() & ~(SelectionKey.OP_READ))
  }

  /**
   *  Tell the reactor that the connection is no longer interested in writing. This is necessary to do it explicitly in
   *  case of an invocation of the write callback that does not write anything to the socket. Otherwise, the reactor will
   *  keep on invoking the write callback.
   *
   * @param rRef A reference to the connection context that was passed to the write callback
   * @return None
   */
  def clearWrite(rRef: ReactorConnectionCtx): AnyRef = {
    val key = rRef.ctx.asInstanceOf[SelectionKey]
    key.interestOps(key.interestOps() & ~(SelectionKey.OP_WRITE))
  }

  def done(rRef: ReactorConnectionCtx): Unit = {
    val key = rRef.ctx.asInstanceOf[SelectionKey]
    val client: SocketChannel = key.channel().asInstanceOf[SocketChannel]
    val reactorClientConnCtx = key.attachment().asInstanceOf[ReactorClientConnCtx]

    clientHandlers.get.disconnectCb(client, reactorClientConnCtx)
    closeConnection(client)
    reactorClientConnCtx.state = Disconnected
  }

  private def selectTimeout = 100

  /**
   * Run the reactor.
   */
  def run(): Unit = {
    statsPrinter()

    while (keepRunning.get()) {
      val readyN = selector.select(selectTimeout)

      while (timers.nonEmpty && (timers.head.isExpired)) {
        val timerCb = timers.dequeue()
        timerCb.call()
      }

      val selectedKeys: util.Set[SelectionKey] = selector.selectedKeys()
      val keyIterator: util.Iterator[SelectionKey] = selectedKeys.iterator()

      while (keyIterator.hasNext) {
        val key: SelectionKey = keyIterator.next();

        if (key.isConnectable) {
          handleConnect(key)
        } else if (key.isWritable) {
          handleWritable(key, key.channel().asInstanceOf[SocketChannel])
        } else if (key.isReadable) {
          handleReadable(key, key.channel().asInstanceOf[SocketChannel])
        }
      }
      selectedKeys.clear()
    }

    stopReactor()
    haltSignal.release()
  }
}
