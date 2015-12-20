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

import static com.google.common.base.Throwables.propagate;

/**
 * Created by devrandom on 2015-Nov-08.
 */
public class StratumChain extends AbstractExecutionThreadService {
    public static final int MAX_REORG = 16384;
    public static final String GET_HEADER = "blockchain.block.get_header";
    public static final String GET_CHUNK = "blockchain.block.get_chunk";
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
            JsonNode result = item.result != null ? item.result : item.params.get(0);
            long height = result.get("block_height").longValue();
            peerHeight = height;
            Block block = makeBlock(result);
            log.info("block {} @{}", height, block.getTime());
            try {
                if (store.getHeight() >= height - 1) {
                    add(block);
                    notifyHeight();
                } else {
                    download(height - 1);
                }
            } catch (CancellationException e) {
                log.error("failed to download chain at height {}", height - 1);
                // Will retry on next time we get a message
            }
            log.info("store is at height {}", store.getHeight());
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

    boolean add(Block block) {
        if (!store.add(block)) {
            try {
                reorg();
            } catch (ExecutionException | InterruptedException e) {
                throw propagate(e);
            }
            return false;
        }
        return true;
    }

    void reorg() throws ExecutionException, InterruptedException {
        long storeHeight = store.getHeight();

        // Find a spot in our local store where the block connects to the block we get from the server.
        // Exponential jumps (1,2,4,8...)
        for (int i = 1 ; i <= MAX_REORG && i <= storeHeight; i += i) {
            Block storePrev = store.get(storeHeight - i);
            log.info("reorg to height {} our prev {}", storeHeight - i + 1, storePrev.getHash());
            ListenableFuture<StratumMessage> future =
                    client.call(GET_HEADER, storeHeight - i + 1);
            StratumMessage item = future.get();
            Block block = makeBlock(item.result);
            if (block.getPrevBlockHash().equals(storePrev.getHash())) {
                // Found the spot.  Truncate blocks beyond it, and add the block from the server.
                store.truncate(storeHeight - i);
                if (!store.add(block))
                    throw new IllegalStateException("could not add block during reorg");
                return;
            }
        }
        // TODO limit reorg to previous checkpoint
        throw new RuntimeException("could not find a reorg point within " + MAX_REORG + " blocks");
    }

    // Keep track of current download, so further calls to download from top level will cancel download in progress
    AtomicLong downloadId = new AtomicLong();

    private void download(final long height) {
        final long id = downloadId.incrementAndGet();
        if (height > store.getHeight() + 50) {
            long index = (store.getHeight() + 1) / NetworkParameters.INTERVAL;
            log.info("at chunk height {}, id {}", index * NetworkParameters.INTERVAL, id);
            ListenableFuture<StratumMessage> future = client.call(GET_CHUNK, index);
            Futures.addCallback(future, new FutureCallback<StratumMessage>() {
                @Override
                public void onSuccess(StratumMessage item) {
                    if (id != downloadId.get()) {
                        log.info("new download started {}, aborting this one", downloadId.get());
                        return;
                    }
                    handleChunk(item);
                    download(height);
                }

                @Override
                public void onFailure(Throwable t) {
                }
            });
        }
        else if (height > store.getHeight()) {
            log.info("adding block, store height={}, id = {}", store.getHeight(), id);
            ListenableFuture<StratumMessage> future = client.call("blockchain.block.get_header", store.getHeight() + 1);
            Futures.addCallback(future, new FutureCallback<StratumMessage>() {
                @Override
                public void onSuccess(StratumMessage item) {
                    if (id != downloadId.get()) {
                        log.info("new download started {}, aborting this one", downloadId.get());
                        return;
                    }
                    handleBlock(item);
                    download(height);
                }

                @Override
                public void onFailure(Throwable t) {
                }
            });
        }
    }

    private void handleBlock(StratumMessage item) {
        Block block = makeBlock(item.result);
        add(block);
        notifyHeight();
    }

    private void handleChunk(StratumMessage item) {
        byte[] data = Utils.HEX.decode(item.result.asText());
        int num = data.length / Block.HEADER_SIZE;
        log.info("chunk size {}", num);
        int start = (int) (store.getHeight() + 1) % NetworkParameters.INTERVAL;
        for (int i = start ; i < num ; i++) {
            Block block = new Block(params, Arrays.copyOfRange(data, i * Block.HEADER_SIZE, (i + 1) * Block.HEADER_SIZE));
            if (!add(block))
                break; // Had a reorg, add one by one at new height
        }
        notifyHeight();
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
