package org.smartwallet.stratum;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.*;
import org.bitcoinj.core.Address;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Created by devrandom on 2015-Aug-25.
 */
public class StratumClient extends AbstractExecutionThreadService {
    public static final int SUBSCRIPTION_QUEUE_CAPACITY = 10;
    public static final int PING_PERIOD = 60;
    protected static Logger logger = LoggerFactory.getLogger("StratumClient");
    private static CycleDetectingLockFactory lockFactory = CycleDetectingLockFactory.newInstance(CycleDetectingLockFactory.Policies.DISABLED);
    protected final ObjectMapper mapper;
    private final ConcurrentMap<Long, PendingCall> calls;
    private final ReentrantLock lock;
    private final ConcurrentMap<String, BlockingQueue<StratumMessage>> subscriptions;
    private final NetworkParameters params;

    protected List<InetSocketAddress> serverAddresses;
    protected Socket socket;
    protected OutputStream outputStream;
    protected BufferedReader reader;
    private boolean isTls;
    private AtomicLong currentId;
    private Map<Address, Long> subscribedAddresses;
    private long subscribedHeaders = 0;
    private Pinger pinger;
    private boolean isQueue;

    static class PendingCall {
        final StratumMessage message;
        final SettableFuture<StratumMessage> future;

        PendingCall(StratumMessage message, SettableFuture<StratumMessage> future) {
            this.message = message;
            this.future = future;
        }

        boolean isSubscriptionCall() {
            return false;
        }
    }

    static class PendingSubscription extends PendingCall {
        PendingSubscription(StratumMessage message, SettableFuture<StratumMessage> future) {
            super(message, future);
        }

        @Override
        boolean isSubscriptionCall() {
            return true;
        }
    }

    static ThreadFactory threadFactory =
            new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                        @Override
                        public void uncaughtException(Thread t, Throwable e) {
                            logger.error("uncaught exception", e);
                        }
                    }).build();
    static ThreadFactory pingerThreadFactory =
            new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("pinger-%d")
                    .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                        @Override
                        public void uncaughtException(Thread t, Throwable e) {
                            logger.error("uncaught exception", e);
                        }
                    }).build();

    public StratumClient() {
        this(NetworkParameters.fromID(NetworkParameters.ID_TESTNET), null, true);
    }

    public StratumClient(NetworkParameters params) {
        this(params, null, true);
    }

    public StratumClient(NetworkParameters params, List<InetSocketAddress> addresses, boolean isTls) {
        this.params = params;
        serverAddresses = (addresses != null) ? addresses : getDefaultAddresses();
        mapper = new ObjectMapper();
        mapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
        currentId = new AtomicLong(1000);
        calls = Maps.newConcurrentMap();
        subscriptions = Maps.newConcurrentMap();
        lock = lockFactory.newReentrantLock("StratumClient-stream");
        this.isTls = isTls;
        subscribedAddresses = Maps.newConcurrentMap();
    }

    private List<InetSocketAddress> getDefaultAddresses() {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        String listName = params.getId().equals(NetworkParameters.ID_MAINNET) ? "electrum-servers" : "electrum-servers-testnet";
        BufferedReader reader = new BufferedReader(new InputStreamReader(classloader.getResourceAsStream(listName)));
        List<InetSocketAddress> addresses = Lists.newArrayList();
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("#"))
                    continue;
                String[] hostPort = line.split(":");
                String host = hostPort[0];
                int port = Integer.parseInt(hostPort[1]);
                addresses.add(InetSocketAddress.createUnresolved(host, port));
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return addresses;
    }

    @Override
    protected Executor executor() {
        return makeExecutor(serviceName());
    }

    private static Executor makeExecutor(final String name) {
        return new Executor() {
            @Override
            public void execute(Runnable command) {
                Thread thread = threadFactory.newThread(command);
                try {
                    thread.setName(name);
                } catch (SecurityException e) {
                    // OK if we can't set the name in this environment.
                }
                thread.start();
            }
        };
    }

    static class TrustAllX509TrustManager implements X509TrustManager {
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }

        public void checkClientTrusted(java.security.cert.X509Certificate[] certs,
                                       String authType) {
        }

        public void checkServerTrusted(java.security.cert.X509Certificate[] certs,
                                       String authType) {
        }
    }

    protected void createSocket() {
        // Lock here, so that caller will call isRunning after we create a new socket
        // and fall out of the loop.  See also closeSocket - called from triggerShutdown.
        lock.lock();
        try {
            if (isTls) {
                try {
                    SSLContext sc = SSLContext.getInstance("TLS");
                    sc.init(null, new TrustManager[]{new TrustAllX509TrustManager()}, new SecureRandom());
                    SocketFactory factory = sc.getSocketFactory();
                    socket = factory.createSocket();
                } catch (NoSuchAlgorithmException | KeyManagementException e) {
                    Throwables.propagate(e);
                }
            } else {
                socket = new Socket();
            }
        } catch (IOException e) {
            // Shouldn't happen, we don't actually connect
            Throwables.propagate(e);
        } finally {
            lock.unlock();
        }
    }

    private void connectSocket() throws IOException {
        // TODO use random, exponentially backoff from failed connections
        InetSocketAddress address = serverAddresses.remove(0);
        serverAddresses.add(address);
        // Force resolution
        address = new InetSocketAddress(address.getHostString(), address.getPort());
        logger.info("Opening a socket to " + address.getHostString() + ":" + address.getPort());
        socket.connect(address); // TODO timeout
    }

    @Override
    protected void startUp() throws Exception {
        createSocket();
    }

    private void connect() throws IOException {
        logger.info("connect");
        pinger = new Pinger();
        connectSocket();
        outputStream = socket.getOutputStream();
        reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        pinger.start();
    }

    class Pinger implements Runnable {
        private final ScheduledExecutorService scheduler =
                Executors.newSingleThreadScheduledExecutor(pingerThreadFactory);
        private ScheduledFuture<?> handle;
        private boolean first = true;
        private ListenableFuture<StratumMessage> future;

        public void start() {
            checkState(handle == null);
            handle = scheduler.scheduleAtFixedRate(this, 0, PING_PERIOD, TimeUnit.SECONDS);
        }

        public void stop() {
            // check if start was never called - e.g. due to an IOException
            if (handle == null)
                return;
            checkNotNull(handle);
            handle.cancel(true);
            if (future != null)
                future.cancel(true);
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(1, TimeUnit.SECONDS))
                    throw new RuntimeException("scheduler did not terminate");
            } catch (InterruptedException e) {
                Throwables.propagate(e);
            }
        }

        @Override
        public void run() {
            try {
                doRun();
            } catch (Throwable t) {
                logger.error("pinger", t);
                Throwables.propagate(t);
            }
        }

        private void doRun() throws InterruptedException, IOException {
            if (future != null) {
                if (future.cancel(true))
                    return; // cancel succeeded means that we timed out
            }
            // Keep a copy of the socket so we don't close a new one due to race in onFailure
            final Socket mySocket = socket;
            future = call("server.version", Lists.<Object>newArrayList("JavaStratumClient 0.1"), false);
            Futures.addCallback(future, new FutureCallback<StratumMessage>() {
                @Override
                public void onSuccess(StratumMessage result) {
                    if (first) {
                        logger.info("server version {}", result.result);
                        first = false;
                    } else
                        logger.info("pong");
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.error("ping failure");
                    try {
                        mySocket.close();
                    } catch (IOException ignored) {
                    }
                }
            });
        }
    }

    @Override
    protected void triggerShutdown() {
        logger.info("trigger shutdown");
        closeSocket();
    }

    private void disconnect() {
        closeSocket();
    }

    public void closeSocket() {
        // See matching lock in createSocket
        lock.lock();
        try {
            socket.close();
        } catch (IOException e) {
            logger.error("failed to close socket", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void shutDown() {
        logger.info("shutdown");
        try {
            lock.lock();
            logger.info("stop pinger");
            pinger.stop();
            logger.info("stopped pinger");
            Exception e = new EOFException("shutting down");
            for (PendingCall value : calls.values()) {
                value.future.setException(e);
            }
            for (BlockingQueue<StratumMessage> queue : subscriptions.values()) {
                try {
                    queue.put(StratumMessage.SENTINEL);
                } catch (InterruptedException e1) {
                    logger.warn("interrupted while trying to queue sentinel");
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @GuardedBy("StratumClient-stream")
    boolean isConnected;

    @Override
    protected void run() {
        while (isRunning()) {
            try {
                connect();
                setConnected(true);
                runClient();
                setConnected(false);
                disconnect();
                createSocket();
            } catch (IOException e) {
                logger.error("IOException", e);
                setConnected(false);
                if (isRunning()) {
                    logger.error("will reconnect");
                    disconnect();
                    createSocket();
                    Utils.sleep(3000 + new Random().nextInt(4000));
                }
            } catch (Throwable t) {
                logger.error("client", t);
                Throwables.propagate(t);
            }
        }
    }

    private void setConnected(boolean value) {
        lock.lock();
        isConnected = value;
        lock.unlock();
    }

    protected void runClient() throws IOException {
        lock.lock();

        try {
            for (Map.Entry<Address, Long> entry : subscribedAddresses.entrySet()) {
                writeMessage(makeMessage("blockchain.address.subscribe", entry.getKey().toString(), entry.getValue()));
            }

            if (subscribedHeaders > 0) {
                writeMessage(makeMessage("blockchain.headers.subscribe", null, subscribedHeaders));
            }

            for (PendingCall call : calls.values()) {
                // Subscriptions are reissued above
                if (!call.isSubscriptionCall())
                    writeMessage(call.message);
            }
        } finally {
            lock.unlock();
        }

        while (true) {
            String line;
            line = reader.readLine();
            logger.info("< {}", line);
            if (line == null) {
                handleFatal(new EOFException());
                return;
            }
            StratumMessage message;
            message = mapper.readValue(line, StratumMessage.class);
            if (message.isResult())
                handleResult(message);
            else if (message.isMessage())
                handleMessage(message);
            else if (message.isError())
                handleError(message);
            else {
                logger.warn("unknown message type");
            }
        }
    }

    public ListenableFuture<StratumMessage> call(String method, String param) {
        return call(method, Lists.<Object>newArrayList(param));
    }

    public ListenableFuture<StratumMessage> call(String method, long param) {
        return call(method, Lists.<Object>newArrayList(param));
    }

    public ListenableFuture<StratumMessage> call(String method, List<Object> params) {
        return call(method, params, isQueue);
    }

    public ListenableFuture<StratumMessage> call(String method, List<Object> params, boolean doQueue) {
        StratumMessage message = new StratumMessage(currentId.getAndIncrement(), method, params, mapper);
        SettableFuture<StratumMessage> future = SettableFuture.create();
        lock.lock();
        try {
            if (!isRunning())
                return null;
            calls.put(message.id, new PendingCall(message, future));
            if (isConnected && !doQueue) {
                writeMessage(message);
            }
        } finally {
            lock.unlock();
        }
        return future;
    }

    /**
     * Subscription call result will be passed into the result queue, with
     * the original subscription params [address] in message.params and the result
     * in message.result.
     *
     * Subscription messages will then be passed into the result queue.
     *
     * @return a subscription, with a future and a message queue
     */
    public StratumSubscription subscribe(Address address) {
        long id = currentId.getAndIncrement();
        subscribedAddresses.put(address, id);
        return subscribe("blockchain.address.subscribe", address.toString(), id);
    }

    /**
     * Subscription call result will be passed into the result queue.
     *
     * Subscription messages will then be passed into the result queue.
     *
     * @return a subscription, with a future and a message queue
     */
    public StratumSubscription subscribeToHeaders() {
        long id = currentId.getAndIncrement();
        subscribedHeaders = id;
        return subscribe("blockchain.headers.subscribe", null, id);
    }

    public void setQueue(boolean isQueue) {
        this.isQueue = isQueue;
    }

    /** Stop async without causing any network access on calling thread */
    public void stopInBackground() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                stopAsync();
            }
        }).start();
    }

    protected StratumSubscription subscribe(String method, String param, long id) {
        try {
            lock.lock();
            if (!subscriptions.containsKey(method)) {
                subscriptions.putIfAbsent(method, makeSubscriptionQueue());
            }
            SettableFuture<StratumMessage> future = SettableFuture.create();
            StratumMessage message = makeMessage(method, param, id);
            calls.put(id, new PendingSubscription(message, future));
            if (isConnected) {
                writeMessage(message);
            }
            return new StratumSubscription(future, subscriptions.get(method));
        } finally {
            lock.unlock();
        }
    }

    private void writeMessage(StratumMessage message) {
        try {
            logger.info("> {}", mapper.writeValueAsString(message));
            mapper.writeValue(outputStream, message);
            outputStream.write('\n');
        } catch (IOException e) {
            logger.error("failed to write, will retry after reconnect");
            // This will be retried by the main loop when we reconnect
        }
    }

    private StratumMessage makeMessage(String method, String param, long id) {
        ArrayList<Object> params = (param != null) ? Lists.<Object>newArrayList(param) : Lists.newArrayList();
        return new StratumMessage(id, method, params, mapper);
    }

    private ArrayBlockingQueue<StratumMessage> makeSubscriptionQueue() {
        return Queues.newArrayBlockingQueue(SUBSCRIPTION_QUEUE_CAPACITY);
    }

    protected void handleResult(StratumMessage message) {
        PendingCall call = calls.remove(message.id);
        if (call == null) {
            logger.warn("reply for unknown id {}", message.id);
            return;
        }
        call.future.set(message);
        // Pass subscription result through queue also
        if (call.message.method.endsWith(".subscribe")) {
            StratumMessage message1 =
                    new StratumMessage(null, call.message.method, call.message.params, message.result, mapper);
            handleMessage(message1);
        }
    }

    protected void handleMessage(StratumMessage message) {
        if (!subscriptions.containsKey(message.method)) {
            logger.warn("message for unknown subscription {}", message.method);
            return;
        }
        try {
            BlockingQueue<StratumMessage> queue = subscriptions.get(message.method);
            queue.put(message);
        } catch (InterruptedException e) {
            logger.warn("interrupted while handling message {}", message.method);
        }
    }

    private void handleError(StratumMessage message) {
        PendingCall call = calls.remove(message.id);
        if (call == null) {
            logger.warn("reply for unknown id {}", message.id);
            return;
        }
        call.future.setException(new StratumException(message.error));
    }

    protected void handleFatal(Exception e) {
        logger.error("exception while connected", e);
        closeSocket();
    }
}
