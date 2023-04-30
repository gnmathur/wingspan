package com.gnmathur.wingspan.core

import io.prometheus.client.Counter

object Statistics {
    val readRequests = Counter.build().name("read_bytes_total").labelNames("endpoint").help("Total bytes read.").register();
    val writeRequest = Counter.build().name("write_bytes_total").labelNames("endpoint").help("Total bytes written.").register();

    def incrementReadRequests(by: Double, labels: String*): Unit = {
        readRequests.labels(labels: _*).inc(by)
    }

    def incrementWriteRequests(by: Double, labels: String*): Unit = {
        writeRequest.labels(labels: _*).inc(by)
    }
}
