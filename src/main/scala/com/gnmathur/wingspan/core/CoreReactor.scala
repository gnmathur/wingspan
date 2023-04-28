package com.gnmathur.wingspan.core

import com.gnmathur.wingspan.core.{NEXT_OP_T, READ_DONE, READ_DONE_WRITE_NEXT, READ_ERROR, READ_MORE, WRITE_DONE, WRITE_DONE_READ_NEXT, WRITE_ERROR, WRITE_MORE}
import org.slf4j.LoggerFactory

import java.io.IOException
import java.net.{InetAddress, InetSocketAddress, SocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, Selector, SocketChannel}
import java.util
import java.util.logging.Logger
import scala.collection.mutable

private case class ClientConnectionContext(clientMetadata: AnyRef, host: String, port: Int)

trait ClientHandlers {
  /**
   * Client callback for when a connection is successfully established
   * @param sc The Socket channel associated with the connection
   * @param clientMetadata Opaque data supplied by the client. It typically encapsulates the client state. Client
   *                       can use this when this callback is invoked to take stateful action
   */
  def connectDoneCb(sc: SocketChannel, clientMetadata: AnyRef): Unit

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
  def readCb(sc: SocketChannel, clientMetadata: AnyRef): NEXT_OP_T
  def readDoneCb(sc: SocketChannel, clientMetadata: AnyRef): Unit
  def readFailCb(cc: SocketChannel, clientMetadata: AnyRef): Unit
  def writeCb(sc: SocketChannel, clientMetadata: AnyRef): NEXT_OP_T
  def writeDoneCb(sc: SocketChannel): Unit
  def writeFailCb(sc: SocketChannel, clientMetadata: AnyRef): Unit
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
      case READ_MORE =>
        require(clientSocket.isConnected)
        logger.debug(s"reading more for ${clientSocket.getRemoteAddress}")
        key.interestOps(SelectionKey.OP_READ)
      case READ_DONE =>
        logger.debug(s"read done for ${clientSocket.getRemoteAddress}")
        clientHandlers.get.readDoneCb(clientSocket, clientConnectionContext.clientMetadata)
        key.interestOps(key.interestOps & (~SelectionKey.OP_READ))
      case READ_DONE_WRITE_NEXT =>
        logger.debug(s"read done write next for ${clientSocket.getRemoteAddress}")
        require(clientSocket.isConnected)
        clientHandlers.get.readDoneCb(clientSocket, clientConnectionContext.clientMetadata)
        key.interestOps(key.interestOps & (~SelectionKey.OP_READ))
        key.interestOps(SelectionKey.OP_WRITE)
      case READ_ERROR =>
        logger.debug(s"read error for ${clientSocket.getRemoteAddress}")
        key.interestOps(key.interestOps & (~SelectionKey.OP_READ))
        clientHandlers.get.readFailCb(clientSocket, clientConnectionContext.clientMetadata)
    }
  }

  private def handleWritable(key: SelectionKey, clientSocket: SocketChannel): Unit = {
    val clientConnectionContext = key.attachment().asInstanceOf[ClientConnectionContext]

    val r = clientHandlers.get.writeCb(clientSocket, clientConnectionContext.clientMetadata)

    key.interestOps(key.interestOps & (~SelectionKey.OP_WRITE))
    r match {
      case WRITE_DONE_READ_NEXT =>
        require(clientSocket.isConnected)
        key.interestOps(SelectionKey.OP_READ)
      case WRITE_DONE =>
        clientHandlers.get.readDoneCb(clientSocket, clientConnectionContext.clientMetadata)
      case WRITE_MORE =>
        require(clientSocket.isConnected)
        key.interestOps(SelectionKey.OP_WRITE)
      case WRITE_ERROR =>
        clientHandlers.get.writeFailCb(clientSocket, clientConnectionContext.clientMetadata)
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
      key.interestOps(SelectionKey.OP_WRITE)
    } catch {
      case e: IOException =>
        clientHandlers.get.connectFailCb(client, ccCtx.clientMetadata)
      case _ => logger.error("Non I/O exception")
    }
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
