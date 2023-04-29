package com.gnmathur.wingspan.core

abstract class EVENT_CB_STATUS_T

case object READ_OK extends EVENT_CB_STATUS_T
case object READ_ERROR extends EVENT_CB_STATUS_T

case object WRITE_MORE extends EVENT_CB_STATUS_T
case object WRITE_DONE extends EVENT_CB_STATUS_T
case object WRITE_ERROR extends EVENT_CB_STATUS_T
case object WRITE_DONE_READ_NEXT extends EVENT_CB_STATUS_T

trait TcpClient {
}
