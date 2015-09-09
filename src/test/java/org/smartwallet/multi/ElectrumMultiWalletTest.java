package org.smartwallet.multi;

import org.bitcoinj.core.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.Before;
import org.junit.Test;
import org.smartwallet.stratum.StratumClient;
import org.smartwallet.stratum.StratumMessage;

import java.net.InetSocketAddress;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

/**
 * Created by devrandom on 2015-09-09.
 */
public class ElectrumMultiWalletTest {
    private NetworkParameters params;
    private SmartWallet wallet;
    private StratumClient client;
    private ElectrumMultiWallet multiWallet;
    private ObjectMapper mapper;

    @Before
    public void setUp() {
        params = NetworkParameters.fromID(NetworkParameters.ID_UNITTESTNET);
        mapper = new ObjectMapper();
        wallet = new SmartWallet(params);
        client = createMock(StratumClient.class);
        multiWallet = new ElectrumMultiWallet(wallet, client);
    }

    @Test
    public void testRetrieveAddressHistory() throws Exception {
        ECKey key = new ECKey();
        String address = key.toAddress(params).toString();
        Transaction tx = new Transaction(params, Utils.HEX.decode("010000000168a93bec585d021cc3382b65e9c2bb95d6c1684f31ef3a3f7102af90ab380262000000008a473044022067f8d6da90ba08db3443e7bbb56a30496deb5ee5384fbc2ac8483ab55e998b14022068786e1460b30cdbb152fcfd85b130c6096496e4a2a9b8277a94016eb186297b014104ccc493c773ed7b190fd3fec0fde94df66605923b5ba6781968921e3f7c86060f62799e085a6873cc5dc1592e99a9090951cad28102cb920da361944d1a827916ffffffff0230517d01000000001976a91492b3870116f135b3d740549bb4785a5f7718b01c88ac40787d01000000001976a914f0dd368cc5ce378301947691548fb9b2c8a0b69088ac00000000"));
        SettableFuture<StratumMessage> addressFuture = SettableFuture.create();
        expect(client.call("blockchain.address.get_history", address)).andReturn(addressFuture);
        SettableFuture<StratumMessage> txFuture = SettableFuture.create();
        expect(client.call("blockchain.transaction.get", tx.getHashAsString()))
                .andReturn(txFuture);
        replay(client);
        multiWallet.retrieveAddressHistory(address);
        JsonNode historyResult = mapper.readTree("[{\"tx_hash\": \"561534ec392fa8eebf5779b233232f7f7df5fd5179c3c640d84378ee6274686b\", \"height\": 340242}]");
        addressFuture.set(new StratumMessage(1L, historyResult));
        txFuture.set(new StratumMessage(2L, mapper.valueToTree(Utils.HEX.encode(tx.bitcoinSerialize()))));
        verify(client);
        assertEquals(1, multiWallet.getTransactions().size());
    }
    
    @Test
    public void testHandleAddressQueueItem() throws Exception {
        ECKey key = new ECKey();
        String address = key.toAddress(params).toString();
        SettableFuture<StratumMessage> addressFuture = SettableFuture.create();
        expect(client.call("blockchain.address.get_history", address)).andReturn(addressFuture);
        replay(client);
        multiWallet.handleAddressQueueItem(new StratumMessage(1L, "blockchain.address.subscribe", Lists.<Object>newArrayList(address), mapper));
        verify(client);
    }
}
