package org.smartwallet.stratum;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.*;
import org.bitcoinj.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;
import static org.smartwallet.stratum.StratumClient.*;

/**
 * Created by devrandom on 2015-Nov-08.
 */
public class StratumChain extends AbstractExecutionThreadService {
    public static final int MAX_REORG = 16384;
    protected static Logger log = LoggerFactory.getLogger("StratumChain");
    private HeadersStore store;
    private BlockingQueue<StratumMessage> queue;
    private final NetworkParameters params;
    private final StratumClient client;
    private final CopyOnWriteArrayList<Listener> listeners;
    private long peerHeight;
    static ThreadFactory threadFactory =
            new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                        @Override
                        public void uncaughtException(Thread t, Throwable e) {
                            log.error("uncaught exception", e);
                        }
                    }).build();

    public void addChainListener(Listener listener) {
        listeners.add(listener);
    }

    public interface Listener {
        void onHeight(long height, Block block, boolean isSynced);
    }

    public StratumChain(NetworkParameters params, HeadersStore store, StratumClient client) {
        this.params = params;
        this.client = client;
        this.store = store;
        listeners = new CopyOnWriteArrayList<>();
    }

    public void close() {
        stopAsync();
        awaitTerminated();
    }

    public long getPeerHeight() {
        return peerHeight;
    }

    @Override
    protected void startUp() throws Exception {
        queue = client.getHeadersQueue();
        super.startUp();
    }

    @Override
    protected void shutDown() throws Exception {
        store.close();
        store = null;
    }

    @Override
    protected void triggerShutdown() {
        queue.offer(StratumMessage.SENTINEL);
    }

    @Override
    protected void run() throws Exception {
        store.verifyLast();
        client.subscribeToHeaders();

        while (true) {
            StratumMessage item = queue.take();
            if (item.isSentinel()) {
                log.info("sentinel on queue, exiting");
                return;
            }
            handleBlock(item);
        }
    }

    boolean handleBlock(StratumMessage item) {
        JsonNode result = item.result != null ? item.result : item.params.get(0);
        long height = result.get("block_height").longValue();
        if (item.method.equals(BLOCKCHAIN_HEADERS_SUBSCRIBE))
            peerHeight = height;
        Block block = makeBlock(result);
        return handleBlock(height, block);
    }

    boolean handleBlock(long height, Block block) {
        long storeHeight = store.getHeight();
        log.info("block {} @{}, ours {}", height, block.getTime(), storeHeight);
        try {
            if (storeHeight > height - 1) {
                // Store is higher - we are in reorg
                Block storePrev = store.get(height - 1);
                // Check if our previous equals peer previous
                if (block.getPrevBlockHash().equals(storePrev.getHash())) {
                    // Found the reorg spot.  Truncate blocks beyond it, and fall through to add the block from the server.
                    store.truncate(height - 1);
                    storeHeight = store.getHeight();
                } else {
                    // Not at the spot, ask block that is twice as old
                    long delta = storeHeight - height + 1;
                    if (delta * 2 > MAX_REORG)
                        throw new RuntimeException("could not find a reorg point within " + MAX_REORG + " blocks");
                    client.call(BLOCKCHAIN_GET_HEADER, height - delta);
                    return false;
                }
            }

            checkState(storeHeight <= height - 1);
            if (storeHeight == height - 1) {
                // We are caught up, add the block
                if (store.add(block)) {
                    client.call(BLOCKCHAIN_GET_HEADER, height + 1); // ask for next
                    notifyHeight();
                    return true;
                } else {
                    // Start a reorg by asking for previous
                    client.call(BLOCKCHAIN_GET_HEADER, height - 1);
                    return false;
                }
            } else {
                // We are not caught up, start or continue download
                download(height - 1);
                return false;
            }
        } catch (CancellationException e) {
            log.error("failed to download chain at height {}", height - 1);
            return false;
            // Will retry on next time we get a message
        }
    }

    private void notifyHeight() {
        boolean isSynced = store.getHeight() >= peerHeight;
        if (isSynced)
            log.info("Synced");

        for (Listener listener : listeners) {
            listener.onHeight(store.getHeight(), store.top(), isSynced);
        }
    }

    // Keep track of current download, so further calls to download from top level will cancel download in progress
    AtomicLong downloadId = new AtomicLong();

    private void download(final long toHeight) {
        final long id = downloadId.incrementAndGet();
        if (toHeight > store.getHeight() + 50) {
            long index = (store.getHeight() + 1) / NetworkParameters.INTERVAL;
            log.info("at chunk height {}, id {}", index * NetworkParameters.INTERVAL, id);
            ListenableFuture<StratumMessage> future = client.call(BLOCKCHAIN_GET_CHUNK, index);
            Futures.addCallback(future, new FutureCallback<StratumMessage>() {
                @Override
                public void onSuccess(StratumMessage item) {
                    if (id != downloadId.get()) {
                        log.info("new download started {}, aborting this one", downloadId.get());
                        return;
                    }
                    if (handleChunk(item))
                        download(toHeight);
                    else {
                        // Ask for next block, just in case server has issues with chunk generation (jelectrum does)
                        log.info("adding block, store height={}, id = {}", store.getHeight(), id);
                        client.call("blockchain.block.get_header", store.getHeight() + 1);
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                }
            });
        }
        else if (toHeight > store.getHeight()) {
            log.info("adding block, store height={}, id = {}", store.getHeight(), id);
            client.call("blockchain.block.get_header", store.getHeight() + 1);
        }
    }

    // Return true if we should continue to next chunk
    private boolean handleChunk(StratumMessage item) {
        byte[] data = Utils.HEX.decode(item.result.asText());
        int num = data.length / Block.HEADER_SIZE;
        log.info("chunk size {}", num);
        long storeHeight = store.getHeight();
        int start = (int) (storeHeight + 1) % NetworkParameters.INTERVAL;
        for (int i = start ; i < num ; i++) {
            Block block = new Block(params, Arrays.copyOfRange(data, i * Block.HEADER_SIZE, (i + 1) * Block.HEADER_SIZE));
            if (!store.add(block)) {
                log.info("need reorg at {}", storeHeight - 1);
                client.call("blockchain.block.get_header", storeHeight - 1); // Initiate a reorg
                return false;
            }
        }
        if (store.getHeight() > storeHeight) {
            notifyHeight();
            return true;
        } else {
            return false;
        }
    }

    private Block makeBlock(JsonNode result) {
        long timestamp = result.get("timestamp").longValue();
        long nonce = result.get("nonce").longValue();
        long difficultyTarget = result.get("bits").longValue();
        long version = result.get("version").longValue();
        Sha256Hash merkle = Sha256Hash.wrap(result.get("merkle_root").asText());
        Sha256Hash prevHash = Sha256Hash.wrap(result.get("prev_block_hash").asText());
        return new Block(params, version, prevHash, merkle, timestamp, difficultyTarget, nonce, Lists.<Transaction>newArrayList()).cloneAsHeader();
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
}
