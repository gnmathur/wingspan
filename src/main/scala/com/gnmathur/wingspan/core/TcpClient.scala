package com.gnmathur.wingspan.core

abstract class NEXT_OP_T

/** There's more data to be read from the channel. Instruct selector to listen for a Read event */
case object READ_MORE extends NEXT_OP_T
/** There's no more data expected to be read from the channel. Instruct selector to clear the Read event */
case object READ_DONE extends NEXT_OP_T
/** There was an error reading data from the channel. Instruct selector to close and cleanup state associated with the channel */
case object READ_ERROR extends NEXT_OP_T
/** Client has read all it needed to from the channel. Instruct selector to listen for a Write event next */
case object READ_DONE_WRITE_NEXT extends NEXT_OP_T

case object WRITE_MORE extends NEXT_OP_T
case object WRITE_DONE extends NEXT_OP_T
case object WRITE_ERROR extends NEXT_OP_T
case object WRITE_DONE_READ_NEXT extends NEXT_OP_T

trait TcpClient {
}
