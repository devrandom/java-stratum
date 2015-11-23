package org.smartwallet.stratum;

import com.google.common.base.Throwables;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.utils.Threading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkState;

/**
 * Created by devrandom on 2015-Nov-08.
 */
public class HeadersStore {
    private static final Logger log = LoggerFactory.getLogger(HeadersStore.class);
    protected final NetworkParameters params;
    protected FileChannel channel;
    protected FileLock fileLock = null;
    protected RandomAccessFile randomFile = null;
    protected ReentrantLock lock = Threading.lock("HeadersStore");

    public HeadersStore(NetworkParameters params, File file) {
        this.params = params;
        try {
            // Set up the backing file.
            randomFile = new RandomAccessFile(file, "rw");
            channel = randomFile.getChannel();
            fileLock = channel.tryLock();
            if (fileLock == null)
                throw new RuntimeException("Store file is already locked by another process");
            if ((randomFile.length() % Block.HEADER_SIZE) != 0) {
                log.warn("file length not round multiple of header size {}", randomFile.length());
                channel.truncate(0);
            }
            channel.position(0);
            if (channel.size() == 0) {
                Block genesis = params.getGenesisBlock().cloneAsHeader();
                channel.write(ByteBuffer.wrap(genesis.bitcoinSerialize()), channel.size());
            }
        } catch (IOException e) {
            if (randomFile != null)
                try {
                    randomFile.close();
                } catch (IOException e1) {
                    Throwables.propagate(e1);
                }
            Throwables.propagate(e);
        }
    }

    /** Get the block at height index. */
    public Block get(long index) {
        lock.lock();
        try {
            if (channel.size() < (index+1) * Block.HEADER_SIZE)
                return null;
            ByteBuffer b = ByteBuffer.allocate(Block.HEADER_SIZE);
            int n = channel.read(b, index * Block.HEADER_SIZE);
            if (n == 0) return null;
            if (n != Block.HEADER_SIZE)
                throw new RuntimeException("partial read from store file");
            return new Block(params, b.array());
        } catch (IOException e) {
            throw Throwables.propagate(e);
        } finally {
            lock.unlock();
        }
    }

    public Block top() {
        lock.lock();
        try {
            return get(getHeight());
        } finally {
            lock.unlock();
        }
    }

    /** Get the height.  A store with just the genesis block is at height zero. */
    public long getHeight() {
        lock.lock();
        try {
            return channel.size() / Block.HEADER_SIZE - 1;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        } finally {
            lock.unlock();
        }
    }

    /** After this call, the store will be at height index. */
    public void truncate(long index) {
        lock.lock();
        try {
            channel.truncate((index + 1) * Block.HEADER_SIZE);
        } catch (IOException e) {
            Throwables.propagate(e);
        } finally {
            lock.unlock();
        }
    }

    public boolean add(Block block) {
        checkState(block.getTransactions() == null);
        lock.lock();
        try {
            if (!block.getPrevBlockHash().equals(top().getHash())) {
                log.error("block.prev = {}, but expecting {}@{}", block.getPrevBlockHash(), top().getHash(), getHeight());
                return false;
            }
            channel.write(ByteBuffer.wrap(block.bitcoinSerialize()), channel.size());
            return true;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        } finally {
            lock.unlock();
        }
    }

    public void verify() {
        verify(0);
    }

    private void verify(long start) {
        long end = getHeight();
        Block last = get(start);
        for (long i = start + 1 ; i <= end ; i++) {
            Block block = get(i);
            if (!block.getPrevBlockHash().equals(last.getHash())) {
                throw new IllegalStateException("invalid at " + i);
            }
            last = block;
        }
    }


    public void verifyLast() {
        long height = getHeight();
        verify(height > 3000 ? height - 3000 : 0);
    }

    public void close() {
        lock.lock();
        try {
            randomFile.close();
        } catch (IOException e) {
            Throwables.propagate(e);
        } finally {
            lock.unlock();
        }
    }
}
