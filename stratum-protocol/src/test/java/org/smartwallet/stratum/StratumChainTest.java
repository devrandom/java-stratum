package org.smartwallet.stratum;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.collect.Lists;
import org.bitcoinj.core.*;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.concurrent.ExecutionException;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

/**
 * Created by devrandom on 2015-Nov-23.
 */
public class StratumChainTest {
    private long nonce = 0;
    private StratumChain chain;
    private StratumClient client;
    private NetworkParameters params;
    private HeadersStore store;

    @Before
    public void setUp() throws IOException {
        File file = File.createTempFile("stratum-chain", ".chain");
        client = createMock(StratumClient.class);
        expect(client.getHeadersQueue()).andStubReturn(null);
        params = NetworkParameters.fromID(NetworkParameters.ID_UNITTESTNET);
        //new CheckpointManager(params, new ByteArrayInputStream("TXT CHECKPOINTS 1\n0\n0\n".getBytes("UTF-8")))
        store = new HeadersStore(params, file, null);
        chain = new StratumChain(params, store, client);
    }

    @Test
    public void checkpoint() throws IOException, ExecutionException, InterruptedException {
        Block block1 = makeBlock(params.getGenesisBlock().getHash());
        Block block2 = makeBlock(block1.getHash());
        File file1 = File.createTempFile("stratum-chain1", ".chain");
        HeadersStore store1 = new HeadersStore(params, file1, new StoredBlock(block1, BigInteger.ZERO, 10));
        StratumChain chain1 = new StratumChain(params, store1, client);
        expect(client.call("blockchain.block.get_header", 12)).andReturn(null);
        replay(client);
        assertEquals(10, store1.getHeight());
        assertTrue(chain1.handleBlock(11, block2));
        assertEquals(11, store1.getHeight());
        assertNotNull(store1.get(0));
        assertNull(store1.get(1));
        assertEquals(block1, store1.get(10));
        assertEquals(block2, store1.get(11));
        verify(client);
    }

    @Test
    public void add() throws ExecutionException, InterruptedException {
        Block block1 = makeBlock(params.getGenesisBlock().getHash());
        Block block2 = makeBlock(block1.getHash());
        expect(client.call("blockchain.block.get_header", 2)).andReturn(null);
        expect(client.call("blockchain.block.get_header", 3)).andReturn(null);
        replay(client);
        assertTrue(chain.handleBlock(1, block1));
        assertTrue(chain.handleBlock(2, block2));
        assertEquals(2, store.getHeight());
        assertEquals(params.getGenesisBlock(), store.get(0));
        assertEquals(block1, store.get(1));
        assertEquals(block2, store.get(2));
        verify(client);
    }

    @Test
    public void reorg() throws ExecutionException, InterruptedException {
        Block block1 = makeBlock(params.getGenesisBlock().getHash());
        Block block1b = makeBlock(params.getGenesisBlock().getHash());
        assertNotEquals(block1.getHash(), block1b.getHash());
        Block block2 = makeBlock(block1.getHash());
        Block block2a = makeBlock(block1.getHash());
        Block block2b = makeBlock(block1b.getHash());
        //immediateFuture(new StratumMessage(0L, blockToJson(block2a)))
        expect(client.call("blockchain.block.get_header", 2)).andReturn(null);
        expect(client.call("blockchain.block.get_header", 3)).andReturn(null);
        expect(client.call("blockchain.block.get_header", 3)).andReturn(null);
        expect(client.call("blockchain.block.get_header", 1)).andReturn(null);
        expect(client.call("blockchain.block.get_header", 2)).andReturn(null);
        expect(client.call("blockchain.block.get_header", 3)).andReturn(null);
        replay(client);
        assertTrue(chain.handleBlock(new StratumMessage(111L, StratumClient.BLOCKCHAIN_GET_HEADER, blockToJson(1, block1)))); // test JSON too
        assertTrue(chain.handleBlock(2, block2));
        assertTrue(chain.handleBlock(2, block2a)); // reorg length 1
        assertEquals(2, store.getHeight());
        assertEquals(store.top(), block2a);
        assertFalse(chain.handleBlock(2, block2b)); // reorg length 2
        assertTrue(chain.handleBlock(1, block1b));
        assertEquals(1, store.getHeight());
        assertEquals(store.top(), block1b);
        assertTrue(chain.handleBlock(2, block2b));
        assertEquals(2, store.getHeight());
        assertEquals(store.top(), block2b);
        verify(client);
    }

    private JsonNode blockToJson(long height, Block block) {
        return JsonNodeFactory.instance.objectNode()
                .put("timestamp", block.getTimeSeconds())
                .put("version", block.getVersion())
                .put("bits", block.getDifficultyTarget())
                .put("nonce", block.getNonce())
                .put("merkle_root", block.getMerkleRoot().toString())
                .put("block_height", height)
                .put("prev_block_hash", block.getPrevBlockHash().toString());
    }

    private Block makeBlock(Sha256Hash prev) {
        return new Block(params, 4, prev, Sha256Hash.ZERO_HASH, 100, 200, nonce++,
                Lists.<Transaction>newArrayList()).cloneAsHeader();
    }
}