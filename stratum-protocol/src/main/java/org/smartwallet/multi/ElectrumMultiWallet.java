package org.smartwallet.multi;

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
import com.google.common.util.concurrent.*;
import com.google.protobuf.ByteString;
import org.bitcoinj.core.*;
import org.bitcoinj.core.TransactionConfidence.ConfidenceType;
import org.bitcoinj.crypto.DeterministicKey;
import org.bitcoinj.script.Script;
import org.bitcoinj.store.UnreadableWalletException;
import org.bitcoinj.utils.ListenerRegistration;
import org.bitcoinj.utils.Threading;
import org.bitcoinj.wallet.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartcolors.*;
import org.smartwallet.stratum.StratumClient;
import org.smartwallet.stratum.StratumMessage;
import org.smartwallet.stratum.StratumSubscription;
import org.smartwallet.stratum.protos.Protos;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

import static com.google.common.base.Preconditions.checkState;

/**
 * Wrap a normal BitcoinJ SPV wallet
 * 
 * Created by devrandom on 2015-09-08.
 */
public class ElectrumMultiWallet extends SmartMultiWallet implements WalletExtension {
    protected static final Logger log = LoggerFactory.getLogger(ElectrumMultiWallet.class);

    public static final String EXTENSION_ID = "org.smartcolors.electrum";

    protected StratumClient client;
    protected final ObjectMapper mapper;
    
    private final Map<Sha256Hash, Transaction> txs;
    private final Set<Sha256Hash> pending;
    private TxConfidenceTable confidenceTable;
    private BlockingQueue<StratumMessage> addressQueue;
    private ExecutorService addressChangeService;
    private transient CopyOnWriteArrayList<ListenerRegistration<MultiWalletEventListener>> eventListeners;

    public ElectrumMultiWallet(SmartWallet wallet) {
        super(wallet);
        if (wallet != null) {
            confidenceTable = getContext().getConfidenceTable();
            wallet.addExtension(this);
        }
        txs = Maps.newConcurrentMap();
        pending = Sets.newConcurrentHashSet();
        mapper = new ObjectMapper();
        eventListeners = new CopyOnWriteArrayList<>();
    }
    
    @Override
    public void addEventListener(MultiWalletEventListener listener, Executor executor) {
        eventListeners.add(new ListenerRegistration<>(listener, executor));
    }

    @Override
    public boolean removeEventListener(MultiWalletEventListener listener) {
        return ListenerRegistration.removeFromList(listener, eventListeners);
    }

    @Override
    public Set<Transaction> getTransactions() {
        return Sets.newHashSet(txs.values());
    }

    @Override
    public Transaction getTransaction(Sha256Hash hash) {
        return txs.get(hash);
    }

    @Override
    public Map<Sha256Hash, Transaction> getTransactionPool(WalletTransaction.Pool pool) {
        // TODO handle other pools?
        if (pool == WalletTransaction.Pool.PENDING)
            return Maps.newHashMap(); // FIXME
        if (pool != WalletTransaction.Pool.UNSPENT)
            throw new UnsupportedOperationException();
        List<TransactionOutput> candidates = calculateAllSpendCandidates(true, false);
        Map<Sha256Hash, Transaction> res = Maps.newHashMap();
        for (TransactionOutput output : candidates) {
            res.put(output.getParentTransactionHash(), output.getParentTransaction());
        }
        return res;
    }

    @Override
    public void markKeysAsUsed(Transaction tx) {
        wallet.lock();
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
                    notifyTransaction(oldTx); // Tell listeners about this again, in case we discovered more outputs are ours
                }
                oldCurrentKeys = currentKeys;
                currentKeys = getCurrentKeys(group);
            }
        } finally {
            wallet.unlockKeychain();
            wallet.unlock();
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
        for (DeterministicKeyChain chain : keychain.getDeterministicKeyChains()) {
            if (chain instanceof ColorKeyChain) {
                ColorKeyChain ckc = (ColorKeyChain) chain;
                list.add(ckc.currentKey(KeyChain.KeyPurpose.RECEIVE_FUNDS));
                list.add(ckc.currentKey(KeyChain.KeyPurpose.CHANGE));
            }
        }
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
        List<TransactionOutput> candidates = Lists.newArrayList();
        Set<TransactionOutPoint> spent = Sets.newHashSet();
        for (Transaction tx : txs.values()) {
            for (TransactionInput input : tx.getInputs()) {
                spent.add(input.getOutpoint());
            }
        }

        for (Transaction tx : txs.values()) {
            for (TransactionOutput output : tx.getOutputs()) {
                if (!spent.contains(output.getOutPointFor())) {
                    if (output.isMine(this)) {
                        candidates.add(output);
                    }
                }
            }
        }
        
        return candidates;
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

    void start(StratumClient mockClient) {
        checkState(client == null);
        this.client = mockClient;
    }

    @Override
    public void startAsync() {
        checkState(client == null);
        client = new StratumClient(wallet.getNetworkParameters());
        // This won't actually cause any network activity yet.  We prefer network activity on the stratum client thread,
        // especially on Android.
        subscribeToKeys();
        client.startAsync();
    }

    @Override
    public void stopAsync() {
        if (client == null) {
            log.warn("already stopped");
            return;
        }
        client.stopInBackground();
        client = null;
    }

    public void stop() {
        if (client == null) {
            log.warn("already stopped");
            return;
        }
        client.stopInBackground();
        log.warn("state is {}", client.state());
        safeAwaitTerminated();
        client = null;
    }

    private void safeAwaitTerminated() {
        if (client.state() == Service.State.FAILED) {
            log.error("client has previously failed", client.failureCause());
        } else {
            try {
                client.awaitTerminated();
            } catch (IllegalStateException e) {
                log.error("client failed during stop", client.failureCause());
            }
        }
    }

    @Override
    public void awaitDownload() throws InterruptedException {
        try {
            List<Integer> res = Futures.allAsList(downloadFutures.values()).get();
            int count = 0;
            for (Integer item : res) {
                count += item;
            }
            log.info("synced {} transactions", count);
        } catch (ExecutionException e) {
            Throwables.propagate(e);
        }
    }

    Map<String, SettableFuture<Integer>> downloadFutures = Maps.newConcurrentMap();

    @VisibleForTesting
    void subscribeToKeys() {
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
        
        for (final Address address : addresses) {
            final String addressString = address.toString();
            downloadFutures.put(addressString, SettableFuture.<Integer>create());
            StratumSubscription subscription = client.subscribe(address);
            addressQueue = subscription.queue;
            listenToAddressQueue();
            Futures.addCallback(subscription.future, new FutureCallback<StratumMessage>() {
                @Override
                public void onSuccess(StratumMessage result) {
                    if (result.result.isNull()) {
                        downloadFutures.get(addressString).set(0);
                    } else {
                        retrieveAddressHistory(addressString);
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    downloadFutures.get(addressString).setException(t);
                }
            });
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

    @Override
    public String getWalletExtensionID() {
        return EXTENSION_ID;
    }

    @Override
    public boolean isWalletExtensionMandatory() {
        return false;
    }

    @Override
    public byte[] serializeWalletExtension() {
        Protos.Electrum.Builder extension = Protos.Electrum.newBuilder();
        for (Transaction tx : txs.values()) {
            TransactionConfidence confidence = tx.getConfidence();
            Protos.TransactionConfidence.Builder confidenceBuilder = Protos.TransactionConfidence.newBuilder();
            confidenceBuilder.setType(Protos.TransactionConfidence.Type.valueOf(confidence.getConfidenceType().getValue()));
            if (confidence.getConfidenceType() == TransactionConfidence.ConfidenceType.BUILDING) {
                confidenceBuilder.setAppearedAtHeight(confidence.getAppearedAtChainHeight());
                confidenceBuilder.setDepth(confidence.getDepthInBlocks());
            }

            Protos.Transaction transaction =
                    Protos.Transaction.newBuilder()
                            .setConfidence(confidenceBuilder)
                            .setTransaction(ByteString.copyFrom(tx.bitcoinSerialize()))
                            .build();
            extension.addTransactions(transaction);
        }
        return extension.build().toByteArray();
    }

    @Override
    public void deserializeWalletExtension(Wallet containingWallet, byte[] data) throws Exception {
        Protos.Electrum extension = Protos.Electrum.parseFrom(data);
        for (Protos.Transaction transaction : extension.getTransactionsList()) {
            Transaction tx = new Transaction(containingWallet.getNetworkParameters(), transaction.getTransaction().toByteArray());
            TransactionConfidence confidence = tx.getConfidence();
            readConfidence(tx, transaction.getConfidence(), confidence);
            txs.put(tx.getHash(), tx);
        }
        this.wallet = (SmartWallet) containingWallet;
        confidenceTable = getContext().getConfidenceTable();
    }

    private void readConfidence(Transaction tx, Protos.TransactionConfidence confidenceProto,
                                TransactionConfidence confidence) throws UnreadableWalletException {
        // We are lenient here because tx confidence is not an essential part of the wallet.
        // If the tx has an unknown type of confidence, ignore.
        if (!confidenceProto.hasType()) {
            log.warn("Unknown confidence type for tx {}", tx.getHashAsString());
            return;
        }
        ConfidenceType confidenceType;
        switch (confidenceProto.getType()) {
            case BUILDING: confidenceType = ConfidenceType.BUILDING; break;
            case DEAD: confidenceType = ConfidenceType.DEAD; break;
            // These two are equivalent (must be able to read old wallets).
            case NOT_IN_BEST_CHAIN: confidenceType = ConfidenceType.PENDING; break;
            case PENDING: confidenceType = ConfidenceType.PENDING; break;
            case UNKNOWN:
                // Fall through.
            default:
                confidenceType = ConfidenceType.UNKNOWN; break;
        }
        confidence.setConfidenceType(confidenceType);
        if (confidenceProto.hasAppearedAtHeight()) {
            if (confidence.getConfidenceType() != TransactionConfidence.ConfidenceType.BUILDING) {
                log.warn("Have appearedAtHeight but not BUILDING for tx {}", tx.getHashAsString());
                return;
            }
            confidence.setAppearedAtChainHeight(confidenceProto.getAppearedAtHeight());
        }
        if (confidenceProto.hasDepth()) {
            if (confidence.getConfidenceType() != TransactionConfidence.ConfidenceType.BUILDING) {
                log.warn("Have depth but not BUILDING for tx {}", tx.getHashAsString());
                return;
            }
            confidence.setDepthInBlocks(confidenceProto.getDepth());
        }
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
                final List<AddressHistoryItem> history;
                try {
                    history = mapper.readValue(mapper.treeAsTokens(result.result), new TypeReference<List<AddressHistoryItem>>() {});
                } catch (IOException e) {
                    log.error("unable to parse history for {}", address);
                    return;
                }

                log.info("got history of length {} for {}", history.size(), address);
                final List<ListenableFuture<StratumMessage>> futures = Lists.newArrayList();
                for (AddressHistoryItem item : history) {
                    Sha256Hash hash = Sha256Hash.wrap(item.txHash);
                    if (txs.containsKey(hash) || pending.contains(hash))
                        continue;
                    pending.add(hash);
                    futures.add(retrieveTransaction(hash, item.height));
                }
                ListenableFuture completeFuture = Futures.allAsList(futures);
                Futures.addCallback(completeFuture, new FutureCallback() {
                    @Override
                    public void onSuccess(Object result) {
                        SettableFuture<Integer> settable = downloadFutures.get(address);
                        if (settable != null && !settable.isDone()) {
                            settable.set(futures.size());
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        SettableFuture<Integer> settable = downloadFutures.get(address);
                        if (settable != null && !settable.isDone()) {
                            settable.setException(t);
                        }
                    }
                });
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                log.error("failed to retrieve {}", address);
            }
        });
    }

    @VisibleForTesting
    ListenableFuture<StratumMessage> retrieveTransaction(final Sha256Hash hash, final int height) {
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
                pending.remove(hash);
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                log.error("failed to retrieve {}", hash);
                pending.remove(hash);
            }
        });
        return future;
    }

    void receive(Transaction tx, int height) {
        TransactionConfidence confidence = confidenceTable.getOrCreate(tx.getHash());
        if (height > 0)
            confidence.setAppearedAtChainHeight(height);
        txs.put(tx.getHash(), tx);
        tx.getConfidence(); // FIXME workaround to Context issue at Fetcher
        log.info("got tx {}", tx.getHashAsString());
        markKeysAsUsed(tx);
        notifyTransaction(tx);
    }

    private void notifyTransaction(final Transaction tx) {
        for (final ListenerRegistration<MultiWalletEventListener> registration : eventListeners) {
            if (registration.executor == Threading.SAME_THREAD) {
                registration.listener.onTransaction(this, tx);
            } else {
                registration.executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        registration.listener.onTransaction(ElectrumMultiWallet.this, tx);
                    }
                });
            }
        }
    }
}
