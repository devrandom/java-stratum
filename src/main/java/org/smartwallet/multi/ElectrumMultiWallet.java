package org.smartwallet.multi;

import org.bitcoinj.core.*;
import org.bitcoinj.script.Script;
import org.bitcoinj.wallet.DeterministicKeyChain;
import org.bitcoinj.wallet.KeyChainGroup;
import org.bitcoinj.wallet.WalletTransaction;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartwallet.stratum.StratumClient;
import org.smartwallet.stratum.StratumMessage;

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
    private BlockingQueue<StratumMessage> addressQueue;
    private ExecutorService addressChangeService;

    public ElectrumMultiWallet(SmartWallet wallet, StratumClient client) {
        this.wallet = wallet;
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
        KeyChainGroup keychain = wallet.getKeychain();
        try {
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
                    SmartWallet.log.warn("Could not parse tx output script: {}", e.toString());
                }
            }
        } finally {
            wallet.unlockKeychain();
        }
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
    
    private void subscribeToKeys() {
        final NetworkParameters params = wallet.getParams();
        List<DeterministicKeyChain> chains = wallet.getKeychain().getDeterministicKeyChains();
        Set<Address> addresses = Sets.newHashSet();
        for (DeterministicKeyChain chain : chains) {
            if (chain instanceof AddressableKeyChain) {
                for (ByteString bytes : ((AddressableKeyChain) chain).getP2SHHashes()) {
                    addresses.add(Address.fromP2SHHash(params, bytes.toByteArray()));
                }
            } else {
                for (ECKey key : chain.getKeys(true)) {
                    addresses.add(key.toAddress(params));
                }
            }
        }
        
        for (Address address : addresses) {
            addressQueue = client.subscribe("blockchain.address.subscribe", address.toString()).queue;
            listenToAddressQueue();
        }
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
        @JsonProperty("height") public long height;
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
                    retrieveTransaction(hash);
                }
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                log.error("failed to retrieve {}", address);
            }
        });
    }

    @VisibleForTesting
    void retrieveTransaction(final Sha256Hash hash) {
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
                txs.put(tx.getHash(), tx);
                log.info("got tx {}", tx.getHashAsString());
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                log.error("failed to retrieve {}", hash);
            }
        });
    }
}
