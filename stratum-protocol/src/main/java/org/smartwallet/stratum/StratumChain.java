package org.smartwallet.stratum;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.bitcoinj.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.*;

/**
 * Created by devrandom on 2015-Nov-08.
 */
public class StratumChain extends AbstractExecutionThreadService {
    protected static Logger log = LoggerFactory.getLogger("StratumChain");
    private HeadersStore store;
    private BlockingQueue<StratumMessage> queue;
    private final NetworkParameters params;
    private final StratumClient client;
    private final CopyOnWriteArrayList<Listener> listeners;
    private final File file;
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
        void onHeight(long height, Block block);
    }

    public StratumChain(NetworkParameters params, File file, StratumClient client) {
        this.params = params;
        this.client = client;
        this.file = file;
        listeners = new CopyOnWriteArrayList<>();
    }

    public HeadersStore getStore() {
        return store;
    }

    public void close() {
        stopAsync();
        awaitTerminated();
    }

    @Override
    protected void shutDown() throws Exception {
        store.close();
    }

    @Override
    protected void triggerShutdown() {
        queue.offer(StratumMessage.SENTINEL);
    }

    @Override
    protected void run() throws Exception {
        store = new HeadersStore(params, file);
        store.verifyLast();
        queue = client.subscribeToHeaders().queue;

        while (true) {
            StratumMessage item = queue.take();
            if (item.isSentinel())
                return;
            long height = item.result.get("block_height").longValue();
            Block block = makeBlock(item.result);
            System.out.println(block);
            try {
                if (download(height - 1) && store.getHeight() == height - 1) {
                    store.add(block);
                }
                for (Listener listener : listeners) {
                    listener.onHeight(height, block);
                }
            } catch (CancellationException | ExecutionException e) {
                log.error("failed to download chain at height {}", height-1);
                // Will retry on next time we get a message
            }
            log.info("store is at height {}", store.getHeight());
        }
    }

    private boolean download(long height) throws InterruptedException, CancellationException, ExecutionException {
        while (height > store.getHeight() + 50) {
            long index = (store.getHeight() + 1) / NetworkParameters.INTERVAL;
            log.info("at chunk height {}", index * NetworkParameters.INTERVAL);
            ListenableFuture<StratumMessage> future = client.call("blockchain.block.get_chunk", index);
            StratumMessage item = future.get();
            long chunkHeight = index * NetworkParameters.INTERVAL;
            byte[] data = Utils.HEX.decode(item.result.asText());
            int num = data.length / Block.HEADER_SIZE;
            int start = (int) (store.getHeight() + 1) % NetworkParameters.INTERVAL;
            for (int i = start ; i < num ; i++) {
                Block block = new Block(params, Arrays.copyOfRange(data, i * Block.HEADER_SIZE, (i+1) * Block.HEADER_SIZE));
                store.add(block);
            }
        }
        while (height > store.getHeight()) {
            System.out.println(store.getHeight());
            ListenableFuture<StratumMessage> future =
                    client.call("blockchain.block.get_header", store.getHeight() + 1);
            StratumMessage item = future.get();
            Block block = makeBlock(item.result);
            store.add(block);
        }
        return true;
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
