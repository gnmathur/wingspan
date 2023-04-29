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

import org.slf4j.LoggerFactory

import java.io.IOException
import java.net.{InetSocketAddress}
import java.nio.channels.SelectionKey.{OP_READ, OP_WRITE}
import java.nio.channels.{SelectionKey, Selector, SocketChannel}
import java.util
import scala.collection.mutable

private case class ClientConnectionContext(clientMetadata: AnyRef, host: String, port: Int)

case class ReactorConnectionContext(ctx: AnyRef)

trait ClientHandlers {

  /**
   * Client callback for when a connection is successfully established
   * @param sc The Socket channel associated with the connection
   * @param connectionContext Opaque state supplied by the reactor. Its supposed to be returned back to the reactor
   *                          in specific reactor APIs
   * @param clientMetadata Opaque data supplied by the client. It typically encapsulates the client state. Client
   *                       can use this when this callback is invoked to take stateful action
   */
  def connectDoneCb(sc: SocketChannel, connectionContext: ReactorConnectionContext, clientMetadata: AnyRef): Unit

  /**
   * Client callback for when a connection fails to establish
   *
   * @param sc Socket channel associated with the connection attempt
   * @param clientMetadata Opaque data supplied by the client. It typically encapsulates the client state. Client
   * can use this when this callback is invoked to take stateful action
   */
  def connectFailCb(sc: SocketChannel, clientMetadata: AnyRef): Unit

  /**
   * Read callback for when there's something to be read from the a connection channel
   *
   * @param sc Socket channel associated with the client connection
   * @param clientMetadata Opaque data supplied by the client. It typically encapsulates the client state. Client
   * can use this when this callback is invoked to take stateful action
   * @return The client instructs what's the next socket operation to set on the selector via type NEXT_OP_T
   */
  def readCb(sc: SocketChannel, clientMetadata: AnyRef): EVENT_CB_STATUS_T
  def readDoneCb(sc: SocketChannel, connectionContext: ReactorConnectionContext, clientMetadata: AnyRef): Unit
  def readFailCb(cc: SocketChannel, connectionContext: ReactorConnectionContext, clientMetadata: AnyRef): Unit
  def writeCb(sc: SocketChannel, clientMetadata: AnyRef): EVENT_CB_STATUS_T
  def writeDoneCb(sc: SocketChannel, connectionContext: ReactorConnectionContext): Unit
  def writeFailCb(sc: SocketChannel, connectionContext: ReactorConnectionContext, clientMetadata: AnyRef): Unit
}

case class TimerCb(
                  timeout: Int,  // in milliseconds
                  timerCb: () => Unit // callback to invoke when the timer expires
                  ) {
  val createdAt = System.currentTimeMillis()
}

class CoreReactor {
  private val selector: Selector = Selector.open()
  private def orderTimers(timerCb: TimerCb) = timerCb.timeout
  private val timers = new mutable.PriorityQueue[TimerCb]()(Ordering.by(orderTimers).reverse)
  private val logger = LoggerFactory.getLogger(classOf[CoreReactor])
  private var clientHandlers: Option[ClientHandlers] = None

  def closeConnection(cc: SocketChannel) = {
    val selectionKey = cc.keyFor(selector)
    selectionKey.cancel()
    cc.close()
  }
  def getConnectionRemoteHostName(cc: SocketChannel): String = cc.getRemoteAddress.asInstanceOf[InetSocketAddress].getHostName
  def getConnectionRemoteHostAddress(cc: SocketChannel): String = {
    if (cc.isConnected)
      cc.getRemoteAddress.asInstanceOf[InetSocketAddress].getAddress.getHostAddress
    else
      "<Unconnected>"
  }
  def getConnectionRemotePort(cc: SocketChannel): Int = cc.getRemoteAddress.asInstanceOf[InetSocketAddress].getPort

  def registerTimer(timerCb: TimerCb): Unit = timers.enqueue(timerCb)

  def registerClient(ch: ClientHandlers): Unit = {
    clientHandlers = Some(ch)
  }

  def connect(host: String, port: Int, clientContext: AnyRef): Unit = {
    val client: SocketChannel = SocketChannel.open();
    client.configureBlocking(false)
    client.connect(new InetSocketAddress(host, port))

    val selectionKey: SelectionKey = client.register(selector, SelectionKey.OP_CONNECT);
    selectionKey.attach(new ClientConnectionContext(clientContext, host, port))
  }

  /**
   *
   * @param key
   * @param clientSocket
   */
  private def handleReadable(key: SelectionKey, clientSocket: SocketChannel): Unit = {
    val clientConnectionContext = key.attachment().asInstanceOf[ClientConnectionContext]

    val r = clientHandlers.get.readCb(clientSocket, clientConnectionContext.clientMetadata)
    r match {
      case READ_OK =>
        require(clientSocket.isConnected)
        clientHandlers.get.readDoneCb(clientSocket, ReactorConnectionContext(key), clientConnectionContext.clientMetadata)
      case READ_ERROR =>
        clientHandlers.get.readFailCb(clientSocket, ReactorConnectionContext(key), clientConnectionContext.clientMetadata)
    }
  }

  private def handleWritable(key: SelectionKey, clientSocket: SocketChannel): Unit = {
    val clientConnectionContext = key.attachment().asInstanceOf[ClientConnectionContext]

    val r = clientHandlers.get.writeCb(clientSocket, clientConnectionContext.clientMetadata)

    key.interestOps(key.interestOps & (~OP_WRITE))
    r match {
      case WRITE_DONE_READ_NEXT =>
        require(clientSocket.isConnected)
      case WRITE_DONE =>
        require(clientSocket.isConnected)
        clientHandlers.get.writeDoneCb(clientSocket, ReactorConnectionContext(key))
      case WRITE_MORE =>
        require(clientSocket.isConnected)
        key.interestOps(OP_WRITE)
      case WRITE_ERROR =>
        clientHandlers.get.writeFailCb(clientSocket, ReactorConnectionContext(key), clientConnectionContext.clientMetadata)
    }
  }

  private def handleConnect(key: SelectionKey): Unit = {
    // Connected to server
    val client: SocketChannel = key.channel().asInstanceOf[SocketChannel]
    val ccCtx = key.attachment().asInstanceOf[ClientConnectionContext]
    try {
      val isConnected = client.finishConnect()
      logger.info("Connected: " + isConnected)
      key.interestOps(key.interestOps & (~SelectionKey.OP_CONNECT))
      clientHandlers.get.connectDoneCb(client, ReactorConnectionContext(key), ccCtx.clientMetadata)
    } catch {
      case e: IOException =>
        clientHandlers.get.connectFailCb(client, ccCtx.clientMetadata)
      case _ => logger.error("Non I/O exception")
    }
  }

  /**
   * Tell the reactor that the connection is ready for writing.
   *
   * @param rRef An opaque reference to the client, that was passed to the write callback
   * @return
   */
  def setWriteReady(rRef: ReactorConnectionContext): Unit = {
    val key = rRef.ctx.asInstanceOf[SelectionKey]
    key.interestOps(OP_WRITE)
  }

  /**
   * Tell the reactor that the connection is ready for reading.
   * @param rRef An opaque reference to the client, that was passed to the read callback
   */
  def setReadReady(rRef: ReactorConnectionContext): Unit = {
    val key = rRef.ctx.asInstanceOf[SelectionKey]
    key.interestOps(OP_READ)
  }

  def clearAll(rRef: ReactorConnectionContext): Unit = {
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
  def clearRead(rRef: ReactorConnectionContext): Unit = {
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
  def clearWrite(rRef: ReactorConnectionContext): AnyRef = {
    val key = rRef.ctx.asInstanceOf[SelectionKey]
    key.interestOps(key.interestOps() & ~(SelectionKey.OP_WRITE))
  }

  def run(): Unit = {
    while (true) {
      val readyN = selector.select(if (timers.isEmpty) 100 else timers.head.timeout)

      while (timers.nonEmpty && (timers.head.createdAt + timers.head.timeout) < System.currentTimeMillis()) {
        val timerCb = timers.dequeue()
        timerCb.timerCb()
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
  }
}
