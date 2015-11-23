package org.smartwallet.stratum;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.collect.Lists;
import org.bitcoinj.core.*;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static com.google.common.util.concurrent.Futures.immediateFuture;
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

    @Before
    public void setUp() throws IOException {
        File file = File.createTempFile("stratum-chain", ".chain");
        client = createMock(StratumClient.class);
        expect(client.getHeadersQueue()).andStubReturn(null);
        params = NetworkParameters.fromID(NetworkParameters.ID_UNITTESTNET);
        chain = new StratumChain(params, file, client);
        chain.createStore();
    }

    @Test
    public void add() throws ExecutionException, InterruptedException {
        Block block1 = makeBlock(params.getGenesisBlock().getHash());
        Block block2 = makeBlock(block1.getHash());
        replay(client);
        assertTrue(chain.add(block1));
        assertTrue(chain.add(block2));
        verify(client);
        assertEquals(2, chain.getStore().getHeight());
    }

    @Test
    public void reorg() throws ExecutionException, InterruptedException {
        Block block1 = makeBlock(params.getGenesisBlock().getHash());
        Block block1b = makeBlock(params.getGenesisBlock().getHash());
        assertNotEquals(block1.getHash(), block1b.getHash());
        Block block2 = makeBlock(block1.getHash());
        Block block2a = makeBlock(block1.getHash());
        Block block2b = makeBlock(block1b.getHash());
        expect(client.call("blockchain.block.get_header", 2)).andReturn(
                immediateFuture(new StratumMessage(0L, blockToJson(block2a))));
        expect(client.call("blockchain.block.get_header", 2)).andReturn(
                immediateFuture(new StratumMessage(0L, blockToJson(block2b))));
        expect(client.call("blockchain.block.get_header", 1)).andReturn(
                immediateFuture(new StratumMessage(0L, blockToJson(block1b))));
        replay(client);
        assertTrue(chain.add(block1));
        assertTrue(chain.add(block2));
        assertFalse(chain.add(block2a));
        assertEquals(2, chain.getStore().getHeight());
        assertEquals(chain.getStore().top(), block2a);
        assertFalse(chain.add(block2b));
        assertEquals(1, chain.getStore().getHeight());
        assertEquals(chain.getStore().top(), block1b);
        assertTrue(chain.add(block2b));
        assertEquals(2, chain.getStore().getHeight());
        assertEquals(chain.getStore().top(), block2b);
        verify(client);
    }

    private JsonNode blockToJson(Block block) {
        return JsonNodeFactory.instance.objectNode()
                .put("timestamp", block.getTimeSeconds())
                .put("version", block.getVersion())
                .put("bits", block.getDifficultyTarget())
                .put("nonce", block.getNonce())
                .put("merkle_root", block.getMerkleRoot().toString())
                .put("prev_block_hash", block.getPrevBlockHash().toString());
    }

    private Block makeBlock(Sha256Hash prev) {
        return new Block(params, 4, prev, Sha256Hash.ZERO_HASH, 100, 200, nonce++,
                Lists.<Transaction>newArrayList()).cloneAsHeader();
    }
}