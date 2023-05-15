package com.gnmathur.wingspan.refapplications.longpollingclient

import com.gnmathur.wingspan.core._
import com.gnmathur.wingspan.refapplications.EchoFrame
import org.slf4j.LoggerFactory

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

/**
 * TODOs
 * 1. Does this need to be a class at all? These are all static objects
 */

object LongPollingClient extends App {
  private abstract class READ_STATE
  private case object READ_NEW extends READ_STATE
  private case object READ_LEN extends READ_STATE

  /** Tracks application state. Handed to the reactor at creation, and is handed to this application in callbacks to
   * let the application retrieve its state.
   */
  private sealed case class ConnectionContext(host: String, port: Int, msg: String) {
    var readState: READ_STATE = READ_NEW
    var readBuffer: ByteBuffer = ByteBuffer.allocate(4)
    var writeBuffer: ByteBuffer = ByteBuffer.allocate(1024)
    var readBytes: Option[Array[Byte]] = None
  }

  private val coreReactor: Reactor = new Reactor()
  new LongPollingClient(coreReactor)
  coreReactor.run()
}

class LongPollingClient(coreReactor: Reactor) extends TcpClient with ClientHandlers {
  import LongPollingClient._

  protected val logger = LoggerFactory.getLogger(classOf[LongPollingClient])

  override def disconnectCb(client: SocketChannel, clientMetadata: AnyRef): Unit = {
    logger.info(s"disconnected from ${coreReactor.getConnectionLocalEndpoint(client)}")
  }

  override def connectDoneCb(sc: SocketChannel, connectionContext: ReactorConnectionCtx, clientMetadata: AnyRef): Unit = {
    logger.info(s"connected to ${coreReactor.getConnectionLocalEndpoint(sc)}")
    coreReactor.setWriteReady(connectionContext)
  }

  override def connectFailCb(sc: SocketChannel, clientMetadata: AnyRef): Unit = {
    val cm = clientMetadata.asInstanceOf[ConnectionContext]
    logger.error(s"failed connection to ${cm.host}:${cm.port}")
  }

  override def readCb(sc: SocketChannel, clientMetadata: AnyRef): EVENT_CB_STATUS_T = {
    val ctx: ConnectionContext = clientMetadata.asInstanceOf[ConnectionContext]
    ctx.readState match {
      case READ_NEW =>
        val bb = ctx.readBuffer
        try {
          val read = sc.read(bb)

          if (read != -1) Statistics.incrementReadRequests(read, sc.getRemoteAddress.toString)

          logger.trace(s"read $read bytes")

          if (read == -1) {
            READ_ERROR
          } else {
            bb.flip()
            val lengthBytes = new Array[Byte](4)
            bb.get(lengthBytes)
            val messageLength = EchoFrame.getFrameLength(lengthBytes)
            ctx.readBuffer = ByteBuffer.allocate(messageLength)
            ctx.readState = READ_LEN
            READ_OK
          }
        } catch {
          case e: IOException =>
            logger.error(e.getMessage)
            ctx.writeBuffer.clear()
            READ_ERROR
        }

      case READ_LEN =>
        val bb = ctx.readBuffer
        val read = sc.read(bb)

        if (read != -1) Statistics.incrementReadRequests(read, sc.getRemoteAddress.toString)
        logger.trace(s"read $read bytes")

        if (read == -1) {
          logger.error("read error")
          READ_ERROR
        } else if (bb.position() != bb.limit()) {
          logger.trace("reading more")
          READ_OK
        } else {
          bb.flip()
          val bytesRead = new Array[Byte](bb.limit())
          bb.get(bytesRead)

          ctx.writeBuffer = ByteBuffer.allocate(1024)
          ctx.readState = READ_NEW
          ctx.readBytes = Some(bytesRead)

          READ_OK
        }
    }
  }

  override def readDoneCb(sc: SocketChannel, reactorConnectionContext: ReactorConnectionCtx, clientMetadata: AnyRef): Unit = {
    val cm = clientMetadata.asInstanceOf[ConnectionContext]
    cm.readState match {
      case READ_NEW =>
        logger.info("read: " + new String(cm.readBytes.get))
        coreReactor.done(reactorConnectionContext)

      case READ_LEN =>
        coreReactor.setReadReady(reactorConnectionContext)
    }
  }

  private def reconnect(myContext: ConnectionContext)(): Unit = {
    logger.info("reconnecting " + myContext.host + " " + myContext.port)
    //coreReactor.registerRequest(myContext.host, myContext.port, myContext)
  }

  override def readFailCb(cc: SocketChannel, reactorConnectionContext: ReactorConnectionCtx, clientMetadata: AnyRef): Unit = {
    coreReactor.clearRead(reactorConnectionContext)
    logger.error("read failed")
  }

  override def writeCb(sc: SocketChannel, clientMetadata: AnyRef): EVENT_CB_STATUS_T = {
    val cm = clientMetadata.asInstanceOf[ConnectionContext]
    val writeBytes = EchoFrame.frameThis(cm.msg.getBytes)

    if (cm.writeBuffer.capacity() < writeBytes.length) {
      // if new request bytes is larger than previous
      cm.writeBuffer = ByteBuffer.allocate(writeBytes.length)
    } else if (writeBytes.length < (0.5 * cm.writeBuffer.capacity())) {
      // if new request bytes is more than 10% smaller than previous. Attempt to reuse a possibly larger ByteBuffer
      // and avoid a reallocation
      cm.writeBuffer = ByteBuffer.allocate(writeBytes.length)
    }
    val bb = ByteBuffer.allocate(writeBytes.length)
    bb.put(writeBytes)
    bb.flip()
    while (bb.hasRemaining) {
      sc.write(bb)
    }
    bb.clear()

    Statistics.incrementWriteRequests(writeBytes.length, sc.getRemoteAddress.toString)
    cm.readBuffer = ByteBuffer.allocate(4)

    WRITE_OK
  }

  override def writeDoneCb(sc: SocketChannel, reactorConnectionContext: ReactorConnectionCtx): Unit = {
    logger.debug("write done")
    coreReactor.setReadReady(reactorConnectionContext)
  }

  override def writeFailCb(sc: SocketChannel, reactorConnectionContext: ReactorConnectionCtx, clientMetadata: AnyRef): Unit = {
    logger.error("write failed")
    coreReactor.clearWrite(reactorConnectionContext)
  }

  private def runClient(periodInMs: Int, host: String, port: Int, msg: String): Unit = {
    coreReactor.registerRequest(host, port, periodInMs, ConnectionContext(host, port, msg))
  }

  coreReactor.registerClient( this)
  runClient(0, "sys76-1", 6770, "Taj Mahal is a wonder of the world. Go see it!!!")
}