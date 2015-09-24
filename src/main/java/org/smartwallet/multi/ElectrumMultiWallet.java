package org.smartwallet.multi;

import org.bitcoinj.core.*;
import org.bitcoinj.crypto.DeterministicKey;
import org.bitcoinj.script.Script;
import org.bitcoinj.wallet.*;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartcolors.AddressableKeyChain;
import org.smartcolors.SmartWallet;
import org.smartwallet.stratum.StratumClient;
import org.smartwallet.stratum.StratumMessage;
import org.smartwallet.stratum.StratumSubscription;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Wrap a normal BitcoinJ SPV wallet
 * 
 * Created by devrandom on 2015-09-08.
 */
public class ElectrumMultiWallet implements MultiWallet {
    protected static final Logger log = LoggerFactory.getLogger(ElectrumMultiWallet.class);

    protected final SmartWallet wallet;
    protected final StratumClient client;
    protected final ObjectMapper mapper;
    
    private final Map<Sha256Hash, Transaction> txs;
    private final TxConfidenceTable confidenceTable;
    private BlockingQueue<StratumMessage> addressQueue;
    private ExecutorService addressChangeService;

    public ElectrumMultiWallet(SmartWallet wallet) {
        this(wallet, new StratumClient());
    }
    
    public ElectrumMultiWallet(SmartWallet wallet, StratumClient client) {
        this.wallet = wallet;
        confidenceTable = wallet.getContext().getConfidenceTable();
        this.client = client;
        txs = Maps.newConcurrentMap();
        mapper = new ObjectMapper();
    }
    
    @Override
    public void addEventListener(WalletEventListener listener, Executor executor) {
        wallet.addEventListener(listener, executor);
    }

    @Override
    public boolean removeEventListener(WalletEventListener listener) {
        return wallet.removeEventListener(listener);
    }

    @Override
    public Set<Transaction> getTransactions() {
        return Sets.newHashSet(txs.values());
    }

    @Override
    public boolean isPubKeyHashMine(byte[] pubkeyHash) {
        return wallet.isPubKeyHashMine(pubkeyHash);
    }

    @Override
    public boolean isWatchedScript(Script script) {
        return wallet.isWatchedScript(script);
    }

    @Override
    public boolean isPubKeyMine(byte[] pubkey) {
        return wallet.isPubKeyHashMine(pubkey);
    }

    @Override
    public boolean isPayToScriptHashMine(byte[] payToScriptHash) {
        return wallet.isPayToScriptHashMine(payToScriptHash);
    }

    @Override
    public Map<Sha256Hash, Transaction> getTransactionPool(WalletTransaction.Pool pool) {
        // FIXME
        throw new UnsupportedOperationException();
    }

    @Override
    public void markKeysAsUsed(Transaction tx) {
        wallet.lockKeychain();
        try {
            KeyChainGroup group = wallet.getKeychain();
            List<Object> oldCurrentKeys = getCurrentKeys(group);
            doMarkKeysAsUsed(tx, group);
            List<Object> currentKeys = getCurrentKeys(group);

            // We may not get txs in topological order, but KCG assumes they are.
            // Re-check any tx that we've already seen that may be topologically later.
            // TODO consider taking height into account to reduce the number of txs we check here.
            // However, also have to consider that when we first ask for txs, we ask by address instead of by
            // height.
            while (!currentKeys.equals(oldCurrentKeys)) {
                for (Transaction oldTx : txs.values()) {
                    doMarkKeysAsUsed(oldTx, group);
                }
                oldCurrentKeys = currentKeys;
                currentKeys = getCurrentKeys(group);
            }
        } finally {
            wallet.unlockKeychain();
        }
    }

    private void doMarkKeysAsUsed(Transaction tx, KeyChainGroup keychain) {
        for (TransactionOutput o : tx.getOutputs()) {
            try {
                Script script = o.getScriptPubKey();
                if (script.isSentToRawPubKey()) {
                    byte[] pubkey = script.getPubKey();
                    keychain.markPubKeyAsUsed(pubkey);
                } else if (script.isSentToAddress()) {
                    byte[] pubkeyHash = script.getPubKeyHash();
                    keychain.markPubKeyHashAsUsed(pubkeyHash);
                } else if (script.isPayToScriptHash()) {
                    Address a = Address.fromP2SHScript(tx.getParams(), script);
                    keychain.markP2SHAddressAsUsed(a);
                }
            } catch (ScriptException e) {
                // Just means we didn't understand the output of this transaction: ignore it.
                log.warn("Could not parse tx output script: {}", e.toString());
            }
        }
    }

    /** A list of current keys / scripts, so that we can detect when they change */
    protected List<Object> getCurrentKeys(KeyChainGroup keychain) {
        List<Object> list = Lists.newArrayList();
        list.add(keychain.currentKey(KeyChain.KeyPurpose.RECEIVE_FUNDS));
        list.add(keychain.currentKey(KeyChain.KeyPurpose.CHANGE));
        return list;
    }

    @Override
    public void completeTx(Wallet.SendRequest req) throws InsufficientMoneyException {
        // FIXME
    }

    @Override
    public void commitTx(Transaction tx) {
        wallet.commitTx(tx);
    }

    @Override
    public List<TransactionOutput> calculateAllSpendCandidates(boolean excludeImmatureCoinbases, boolean excludeUnsignable) {
        return wallet.calculateAllSpendCandidates(excludeImmatureCoinbases, excludeUnsignable);
    }

    @Override
    public ListenableFuture<Transaction> broadcastTransaction(final Transaction tx) {
        ListenableFuture<StratumMessage> future = client.call("blockchain.transaction.broadcast", Utils.HEX.encode(tx.bitcoinSerialize()));
        return Futures.transform(future, new Function<StratumMessage, Transaction>() {
            @Override
            public Transaction apply(StratumMessage input) {
                return tx;
            }
        });
    }

    @Override
    public void start() {
        startAsync();
        client.awaitRunning();
    }

    @Override
    public void startAsync() {
        downloadFuture = subscribeToKeys();
        client.startAsync();
    }

    @Override
    public void awaitDownload() throws InterruptedException {
        try {
            downloadFuture.get();
        } catch (ExecutionException e) {
            Throwables.propagate(e);
        }
    }

    ListenableFuture<List<StratumMessage>> downloadFuture;

    @VisibleForTesting
    ListenableFuture<List<StratumMessage>> subscribeToKeys() {
        final NetworkParameters params = wallet.getParams();
        List<DeterministicKeyChain> chains = wallet.getKeychain().getDeterministicKeyChains();
        Set<Address> addresses = Sets.newHashSet();
        for (DeterministicKeyChain chain : chains) {
            chain.maybeLookAhead();
            if (chain instanceof AddressableKeyChain) {
                for (ByteString bytes : ((AddressableKeyChain) chain).getP2SHHashes()) {
                    addresses.add(Address.fromP2SHHash(params, bytes.toByteArray()));
                }
            } else {
                for (ECKey ecKey : chain.getLeafKeys()) {
                    DeterministicKey key = (DeterministicKey) ecKey;
                    addresses.add(key.toAddress(params));
                }
            }
        }
        
        List<ListenableFuture<StratumMessage>> futures = Lists.newArrayList();
        for (Address address : addresses) {
            StratumSubscription subscription = client.subscribe(address);
            addressQueue = subscription.queue;
            futures.add(subscription.future);
            listenToAddressQueue();
        }
        return Futures.allAsList(futures);
    }

    private void listenToAddressQueue() {
        if (addressChangeService == null) {
            addressChangeService = Executors.newSingleThreadExecutor();
            addressChangeService.submit(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            StratumMessage item = addressQueue.take();
                            if (item.isSentinel()) {
                                addressChangeService.shutdown();
                                break;
                            }
                            log.info(mapper.writeValueAsString(item));
                            handleAddressQueueItem(item);
                        } catch (InterruptedException | JsonProcessingException e) {
                            throw Throwables.propagate(e);
                        }
                    }
                }
            });
        }
    }

    @VisibleForTesting
    void handleAddressQueueItem(StratumMessage item) throws JsonProcessingException {
        if (item.params.size() < 1) {
            log.error("got address subscription update with no params");
            return;
        }
        final String address = item.params.get(0).asText();
        if (address.isEmpty()) {
            log.error("got address subscription update with no address");
            return;
        }
        retrieveAddressHistory(address);
    }

    static class AddressHistoryItem {
        @JsonProperty("tx_hash") public String txHash;
        
        @SuppressWarnings("unused")
        @JsonProperty("height") public int height;
    }

    @VisibleForTesting
    void retrieveAddressHistory(final String address) {
        ListenableFuture<StratumMessage> future = client.call("blockchain.address.get_history", address);
        Futures.addCallback(future, new FutureCallback<StratumMessage>() {
            @Override
            public void onSuccess(StratumMessage result) {
                List<AddressHistoryItem> history;
                try {
                    history = mapper.readValue(mapper.treeAsTokens(result.result), new TypeReference<List<AddressHistoryItem>>() {});
                } catch (IOException e) {
                    log.error("unable to parse history for {}", address);
                    return;
                }

                log.info("got history of length {} for {}", history.size(), address);
                for (AddressHistoryItem item : history) {
                    Sha256Hash hash = Sha256Hash.wrap(item.txHash);
                    if (txs.containsKey(hash))
                        return;
                    retrieveTransaction(hash, item.height);
                }
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                log.error("failed to retrieve {}", address);
            }
        });
    }

    @VisibleForTesting
    void retrieveTransaction(final Sha256Hash hash, final int height) {
        ListenableFuture<StratumMessage> future = client.call("blockchain.transaction.get", hash.toString());
        Futures.addCallback(future, new FutureCallback<StratumMessage>() {
            @Override
            public void onSuccess(StratumMessage result) {
                String hex = result.result.asText();
                if (hex.isEmpty()) {
                    log.error("unable to parse transaction for " + hash);
                    return;
                }
                // FIXME check proof
                Transaction tx = new Transaction(wallet.getParams(), Utils.HEX.decode(hex));
                receive(tx, height);
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                log.error("failed to retrieve {}", hash);
            }
        });
    }

    void receive(Transaction tx, int height) {
        TransactionConfidence confidence = confidenceTable.getOrCreate(tx.getHash());
        confidence.setAppearedAtChainHeight(height);
        txs.put(tx.getHash(), tx);
        log.info("got tx {}", tx.getHashAsString());
        markKeysAsUsed(tx);
    }
}
