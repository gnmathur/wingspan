Wingspan
========
# Introduction
A Java NIO based framework to write low-footprint request-response applications. There are reference applications to 
demonstrate the use of the framework.

The framework provides callbacks to handle the following events:
* Connection established
* Connection closed
* Request sent to remote endpoint
* Request send failure
* Response received from remote endpoint
* Response receive failure

# Low-Level Design
* Clients can register to the framework and each client can have multiple connections to remote endpoints
* The framework uses timers to queue requests to remote endpoints. The timer is a single thread and is shared by all
* The framework is based on Java NIO and uses non-blocking IO to read and write data from the socket. It uses a single 
thread to establish a connection to a remote endpoint, and to read, write data from a socket 
* A read or write failure is treated as a connection failure and the connection is closed. A new request is queued up 
after the failure
* 
# Build and run
```
➜  ~/wkspcs/projects/wingspan
> git:(main) ✗ sbt clean assembly && java -cp target/scala-2.13/wingspan-assembly-0.1.0-SNAPSHOT.jar com.gnmathur.wingspan.refapplications.TimedFramedEchoClient 
```
