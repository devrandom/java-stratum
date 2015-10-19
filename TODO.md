Reconnect race issue

    09:25:54 26 StratumClient.call: > {"id":6808,"method":"server.version","params":["JavaStratumClient 0.1"]}
    09:25:54 12 StratumClient.runClient: < {"id":6808,"result":"1.0","jelectrum":"beancurd"}
    09:25:54 12 StratumClient$Pinger$1.onSuccess: pong
    09:25:56 12 StratumClient.runClient: < null
    09:25:56 12 StratumClient.handleFatal: exception while connected
    java.io.EOFException
        at org.smartwallet.stratum.StratumClient.runClient(StratumClient.java:342)
        at org.smartwallet.stratum.StratumClient.run(StratumClient.java:300)
        at com.google.common.util.concurrent.AbstractExecutionThreadService$1$2.run(AbstractExecutionThreadService.java:60)
        at com.google.common.util.concurrent.Callables$3.run(Callables.java:95)
        at java.lang.Thread.run(Thread.java:745)
    09:25:56 12 StratumClient.disconnect: stop pinger
    09:25:56 12 StratumClient.disconnect: stopped pinger
    09:25:56 12 StratumClient.connect: connect
    09:25:56 12 StratumClient.createSocket: Opening a socket to h.1209k.com:50004
    09:25:56 12 StratumClient.run: IO exception, will reconnect
    09:25:56 12 StratumClient.disconnect: stop pinger
    09:25:56 12 StratumClient.shutDown: shutdown
    09:25:56 12 StratumClient$1.uncaughtException: uncaught exception
    java.lang.NullPointerException
        at com.google.common.base.Preconditions.checkNotNull(Preconditions.java:210)
        at org.smartwallet.stratum.StratumClient$Pinger.stop(StratumClient.java:201)
        at org.smartwallet.stratum.StratumClient.disconnect(StratumClient.java:261)
        at org.smartwallet.stratum.StratumClient.run(StratumClient.java:315)
        at com.google.common.util.concurrent.AbstractExecutionThreadService$1$2.run(AbstractExecutionThreadService.java:60)
        at com.google.common.util.concurrent.Callables$3.run(Callables.java:95)
        at java.lang.Thread.run(Thread.java:745)
