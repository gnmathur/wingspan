package com.gnmathur.wingspan.refapplications

import com.gnmathur.wingspan.core.{NEXT_OP_T, READ_DONE_WRITE_NEXT, READ_ERROR, READ_MORE, TcpClient, WRITE_DONE_READ_NEXT}
import com.gnmathur.wingspan.core.{ClientHandlers, CoreReactor, TimerCb}
import org.slf4j.LoggerFactory

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, SocketChannel}

/**
 * TODOs
 * 1. Does this need to be a class at all? These are all static objects
 */

abstract class READ_STATE
case object READ_NEW extends READ_STATE
case object READ_LEN extends READ_STATE

private sealed case class ConnectionContext( host: String, port: Int, msg: String) {
  var readState: READ_STATE = READ_NEW
  var readBuffer: ByteBuffer = ByteBuffer.allocate(4)
  var writeBuffer: ByteBuffer = ByteBuffer.allocate(1024)
  var readBytes: Option[Array[Byte]] = None
}

class FramedEchoClient(coreReactor: CoreReactor) extends TcpClient with ClientHandlers {
  private val logger = LoggerFactory.getLogger(classOf[FramedEchoClient])

  override def connectDoneCb(sc: SocketChannel, clientMetadata: AnyRef): Unit = {
    logger.info(s"connected to ${coreReactor.getConnectionRemoteHostAddress(sc)}")
  }

  override def connectFailCb(sc: SocketChannel, clientMetadata: AnyRef): Unit = {
    val cm = clientMetadata.asInstanceOf[ConnectionContext]
    logger.error(s"failed connection to ${cm.host}:${cm.port}")
    coreReactor.closeConnection(sc)

    coreReactor.registerTimer(new TimerCb(5000, () => {
      coreReactor.connect(cm.host, cm.port, clientMetadata)
    }))
  }

  override def readCb(sc: SocketChannel, clientMetadata: AnyRef): NEXT_OP_T = {
    val ctx: ConnectionContext = clientMetadata.asInstanceOf[ConnectionContext]
    ctx.readState match {
      case READ_NEW =>
        val bb = ctx.readBuffer
        try {
          val read = sc.read(bb)
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
            READ_MORE
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
        logger.trace(s"read $read bytes")

        if (read == -1) {
          logger.error("read error")
          READ_ERROR
        } else if (bb.position() != bb.limit()) {
          logger.trace("reading more")
          READ_MORE
        } else {
          bb.flip()
          val bytesRead = new Array[Byte](bb.limit())
          bb.get(bytesRead)

          ctx.writeBuffer = ByteBuffer.allocate(1024)
          ctx.readState = READ_NEW
          ctx.readBytes = Some(bytesRead)
          READ_DONE_WRITE_NEXT
        }
    }
  }

  override def readDoneCb(sc: SocketChannel, clientMetadata: AnyRef): Unit = {
    val cm = clientMetadata.asInstanceOf[ConnectionContext]
    logger.debug("read done")
    logger.info("read: " + new String(cm.readBytes.get))
    //logger.info("read: " + cm.readBytes.get.length)
  }

  private def reconnect(myContext: ConnectionContext)(): Unit = {
    logger.info("reconnecting " + myContext.host + " " + myContext.port)
    coreReactor.connect(myContext.host, myContext.port, myContext)
  }

  override def readFailCb(cc: SocketChannel, clientMetadata: AnyRef): Unit = {
    logger.error("read failed")
    coreReactor.registerTimer(new TimerCb(10000, reconnect(clientMetadata.asInstanceOf[ConnectionContext])))
  }

  override def writeCb(sc: SocketChannel, clientMetadata: AnyRef): NEXT_OP_T = {
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

    cm.readBuffer = ByteBuffer.allocate(4)

    WRITE_DONE_READ_NEXT
  }

  override def writeDoneCb(sc: SocketChannel): Unit = logger.debug("write done")

  override def writeFailCb(sc: SocketChannel, clientMetadata: AnyRef): Unit = logger.error("write failed")

  coreReactor.registerClient(this)

  //
  private val source = scala.io.Source.fromResource("odyssey.txt")
  private val lines = try source.mkString finally source.close()

  // coreReactor.connect("sys76-1", 6770, ConnectionContext("sys76-1", 6770, "Taj Mahal is a wonder of the world!"))
  // coreReactor.connect("sys76-1", 6771, ConnectionContext("sys76-1", 6771, "West is west of east"))
  // coreReactor.connect("sys76-1", 6772, ConnectionContext("sys76-1", 6772, "Washington DC is the capital of the US"))
  (0 until 299) foreach { x =>
    coreReactor.connect("sys76-1", 10000+x, ConnectionContext("sys76-1", 10000+x, s"We go ${10000+x} steps in the right direction"))
  }
}

object FramedEchoClient extends App {
  private val coreReactor: CoreReactor = new CoreReactor()
  new FramedEchoClient(coreReactor)
  coreReactor.run()
}
