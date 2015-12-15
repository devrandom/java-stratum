package org.smartwallet.multi;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
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
import org.smartwallet.stratum.*;
import org.smartwallet.stratum.protos.Protos;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;

import static com.google.common.base.Preconditions.*;

/**
 * Wrap a normal BitcoinJ SPV wallet
 * 
 * Created by devrandom on 2015-09-08.
 */
public class ElectrumMultiWallet extends SmartMultiWallet implements WalletExtension, StratumChain.Listener {
    protected static final Logger log = LoggerFactory.getLogger(ElectrumMultiWallet.class);

    public static final int RESET_HEIGHT = 300000;
    public static final String EXTENSION_ID = "org.smartcolors.electrum";

    private static final Map<Sha256Hash, Transaction> EMPTY_POOL = Maps.newHashMap();
    public static final int CHECKPOINT_TIME_BUFFER = 3600 * 24 * 1;
    private final File baseDirectory;
    private HeadersStore store;

    protected StratumClient client;
    protected final ObjectMapper mapper;
    
    private final Map<Sha256Hash, Transaction> txs;
    private final ConcurrentMap<Sha256Hash, SettableFuture<Transaction>> pendingDownload;
    private SortedSet<TransactionWithHeight> pendingBlock;
    private TxConfidenceTable confidenceTable;
    private BlockingQueue<StratumMessage> addressQueue;
    private ExecutorService addressChangeService;
    private transient CopyOnWriteArrayList<ListenerRegistration<MultiWalletEventListener>> eventListeners;
    private StratumChain chain;
    private boolean isChainSynced;
    private boolean isHistorySynced;
    private ListenableFuture<List<Integer>> downloadFuture;
    private CheckpointManager checkpoints;

    /**
     * The constructor will add this object as an extension to the wallet.
     *
     * @param wallet
     * @param baseDirectory the chain file will be stored here
     */
    public ElectrumMultiWallet(SmartWallet wallet, File baseDirectory) {
        super(wallet);
        confidenceTable = getContext().getConfidenceTable();
        wallet.addExtension(this);
        txs = Maps.newConcurrentMap();
        pendingDownload = Maps.newConcurrentMap();
        pendingBlock = Sets.newTreeSet();
        mapper = new ObjectMapper();
        eventListeners = new CopyOnWriteArrayList<>();
        this.baseDirectory = baseDirectory;
        wallet.saveNow();
    }

    public ElectrumMultiWallet(SmartWallet wallet) {
        this(wallet, null);
    }
    
    @Override
    public void addEventListener(MultiWalletEventListener listener, Executor executor) {
        eventListeners.add(new ListenerRegistration<>(listener, executor));
    }

    public void setCheckpoints(CheckpointManager checkpoints) {
        this.checkpoints = checkpoints;
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
        if (pool == WalletTransaction.Pool.UNSPENT)
            return txs;
        return EMPTY_POOL;
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
        checkArgument(req.coinSelector instanceof BitcoinCoinSelector, "Must provide a BitcoinCoinSelector");
        wallet.lock();
        try {
            // Calculate the amount of value we need to import.
            Coin value = Coin.ZERO;
            for (TransactionOutput output : req.tx.getOutputs()) {
                value = value.add(output.getValue());
            }

            log.info("Completing send tx with {} outputs totalling {} (not including fees)",
                    req.tx.getOutputs().size(), value.toFriendlyString());

            // If any inputs have already been added, we don't need to get their value from wallet
            Coin totalInput = Coin.ZERO;
            for (TransactionInput input : req.tx.getInputs())
                if (input.getConnectedOutput() != null)
                    totalInput = totalInput.add(input.getConnectedOutput().getValue());
                else
                    log.warn("SendRequest transaction already has inputs but we don't know how much they are worth - they will be added to fee.");
            value = value.subtract(totalInput);

            List<TransactionInput> originalInputs = new ArrayList<TransactionInput>(req.tx.getInputs());

            // We need to know if we need to add an additional fee because one of our values are smaller than 0.01 BTC
            boolean needAtLeastReferenceFee = false;
            if (req.ensureMinRequiredFee) { // min fee checking is handled later for emptyWallet
                for (TransactionOutput output : req.tx.getOutputs())
                    if (output.getValue().compareTo(Coin.CENT) < 0) {
                        if (output.getValue().compareTo(output.getMinNonDustValue()) < 0)
                            throw new Wallet.DustySendRequested();
                        needAtLeastReferenceFee = true;
                        break;
                    }
            }

            // Calculate a list of ALL potential candidates for spending and then ask a coin selector to provide us
            // with the actual outputs that'll be used to gather the required amount of value. In this way, users
            // can customize coin selection policies.
            //
            // Note that this code is poorly optimized: the spend candidates only alter when transactions in the wallet
            // change - it could be pre-calculated and held in RAM, and this is probably an optimization worth doing.
            List<TransactionOutput> candidates = calculateAllSpendCandidates(true, false);

            CoinSelection bestCoinSelection;
            FeeCalculator.FeeCalculation feeCalculation;
            feeCalculation = FeeCalculator.calculateFee(this, req, value, originalInputs, needAtLeastReferenceFee, candidates);
            bestCoinSelection = feeCalculation.bestCoinSelection;
            TransactionOutput bestChangeOutput = feeCalculation.bestChangeOutput;

            for (TransactionOutput output : bestCoinSelection.gathered)
                req.tx.addInput(output);

            if (bestChangeOutput != null) {
                req.tx.addOutput(bestChangeOutput);
                log.info("  with {} change", bestChangeOutput.getValue().toFriendlyString());
            }

            // Now shuffle the outputs to obfuscate which is the change.
            if (req.shuffleOutputs)
                req.tx.shuffleOutputs();

            // Now sign the inputs, thus proving that we are entitled to redeem the connected outputs.
            if (req.signInputs) {
                wallet.signTransaction(req);
            }

            // Check size.
            int size = req.tx.bitcoinSerialize().length;
            if (size > Transaction.MAX_STANDARD_TX_SIZE)
                throw new Wallet.ExceededMaxTransactionSize();

            final Coin calculatedFee = req.tx.getFee();
            if (calculatedFee != null) {
                log.info("  with a fee of {}", calculatedFee.toFriendlyString());
            }

            // Label the transaction as being self created. We can use this later to spend its change output even before
            // the transaction is confirmed. We deliberately won't bother notifying listeners here as there's not much
            // point - the user isn't interested in a confidence transition they made themselves.
            req.tx.getConfidence(wallet.getContext()).setSource(TransactionConfidence.Source.SELF);
            // Label the transaction as being a user requested payment. This can be used to render GUI wallet
            // transaction lists more appropriately, especially when the wallet starts to generate transactions itself
            // for internal purposes.
            req.tx.setPurpose(Transaction.Purpose.USER_PAYMENT);
            // Record the exchange rate that was valid when the transaction was completed.
            req.tx.setExchangeRate(req.exchangeRate);
            req.tx.setMemo(req.memo);
            req.fee = calculatedFee;
            log.info("  completed: {}", req.tx);
        } finally {
            wallet.unlock();
        }
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
        ListenableFuture<StratumMessage> future = client.callAsync("blockchain.transaction.broadcast", Utils.HEX.encode(tx.bitcoinSerialize()));
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

    void start(StratumClient mockClient, StratumChain mockChain, HeadersStore mockStore) {
        checkState(client == null);
        client = mockClient;
        chain = mockChain;
        store = mockStore;
    }

    @Override
    public void startAsync() {
        checkState(client == null);
        checkState(chain == null);
        checkState(addressQueue == null);
        checkState(addressChangeService == null);

        client = new StratumClient(wallet.getNetworkParameters());
        store = makeStore();
        chain = makeChain(client);
        chain.addChainListener(this);
        // This won't actually cause any network activity yet.  We prefer network activity on the stratum client thread,
        // especially on Android.

        addressQueue = client.getAddressQueue();
        listenToAddressQueue(addressQueue);

        subscribeToKeys();
        chain.startAsync();
        client.startAsync();
    }

    private HeadersStore makeStore() {
        StoredBlock checkpoint = null;
        if (checkpoints != null) {
            checkpoint = checkpoints.getCheckpointBefore(wallet.getEarliestKeyCreationTime() - CHECKPOINT_TIME_BUFFER);
        }
        return new HeadersStore(wallet.getNetworkParameters(), getChainFile(), checkpoint);
    }

    private StratumChain makeChain(StratumClient client) {
        return new StratumChain(wallet.getNetworkParameters(), store, client);
    }

    private File getChainFile() {
        return new File(baseDirectory, "electrum.chain");
    }

    @Override
    public void stopAsync() {
        if (client == null) {
            log.warn("already stopped");
            return;
        }
        chain.stopAsync();
        chain = null;
        client.stopInBackground();
        client = null;
        store.close();
        store = null;
    }

    public void stop() {
        if (client == null) {
            log.warn("already stopped");
            return;
        }
        chain.close();
        chain = null;
        client.stopInBackground();
        log.warn("state is {}", client.state());
        safeAwaitTerminated();
        client = null;
        store.close();
        store = null;
        doneWithAddressQueue();
    }

    public void safeAwaitTerminated() {
        // Await for client to terminate
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

    // Await for service to terminate.  It should have terminated, since the client should have told it to
    // shut down.
    private void doneWithAddressQueue() {
        try {
            boolean terminated = addressChangeService.awaitTermination(100, TimeUnit.SECONDS);
            if (!terminated)
                throw new IllegalStateException("could not stop addressChangeService after 100 sec");
        } catch (InterruptedException e) {
            Throwables.propagate(e);
        }
        addressChangeService = null;
        addressQueue = null;
    }

    @Override
    public void awaitDownload() throws InterruptedException {
        try {
            checkNotNull(downloadFuture);
            List<Integer> res = downloadFuture.get();
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
            client.subscribe(address);
        }

        downloadFuture = Futures.allAsList(downloadFutures.values());
        Futures.addCallback(downloadFuture, new FutureCallback<List<Integer>>() {
            @Override
            public void onSuccess(List<Integer> result) {
                isHistorySynced = true;
                notifyHeight(store.getHeight());
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("failed to sync history", t);
            }
        });
    }

    static ThreadFactory historyThreadFactory =
            new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("address-%d")
                    .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                        @Override
                        public void uncaughtException(Thread t, Throwable e) {
                            log.error("uncaught exception", e);
                        }
                    }).build();

    private void listenToAddressQueue(final BlockingQueue<StratumMessage> queue) {
        if (addressChangeService == null) {
            addressChangeService = Executors.newSingleThreadExecutor(historyThreadFactory);
            addressChangeService.submit(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            StratumMessage item = queue.take();
                            if (item.isSentinel()) {
                                addressChangeService.shutdown();
                                log.info("sentinel on queue, exiting");
                                break;
                            }
                            log.info(mapper.writeValueAsString(item));
                            handleAddressQueueItem(item);
                        } catch (Throwable t) {
                            log.error("address change service", t);
                            throw Throwables.propagate(t);
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
        JsonNode result = item.result;
        if (result == null)
            result = item.params.get(1);
        if (result.isNull()) {
            SettableFuture<Integer> future = downloadFutures.get(address);
            if (future != null && !future.isDone()) {
                future.set(0);
            }
        } else {
            retrieveAddressHistory(address);
        }
    }

    @Override
    public String getWalletExtensionID() {
        return EXTENSION_ID;
    }

    @Override
    public boolean isWalletExtensionMandatory() {
        return true;
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

            Protos.Transaction.Builder transaction =
                    Protos.Transaction.newBuilder()
                            .setConfidence(confidenceBuilder)
                            .setTransaction(ByteString.copyFrom(tx.bitcoinSerialize()));
            if (tx.getUpdateTime() != null)
                transaction.setUpdatedAt(tx.getUpdateTime().getTime());
            extension.addTransactions(transaction.build());
        }
        return extension.build().toByteArray();
    }

    @Override
    public void deserializeWalletExtension(Wallet containingWallet, byte[] data) throws Exception {
        Protos.Electrum extension = Protos.Electrum.parseFrom(data);
        for (Protos.Transaction transaction : extension.getTransactionsList()) {
            Transaction tx = new Transaction(containingWallet.getNetworkParameters(), transaction.getTransaction().toByteArray());
            TransactionConfidence confidenceProto = tx.getConfidence();
            readConfidence(tx, transaction.getConfidence(), confidenceProto);
            txs.put(tx.getHash(), tx);
            if (transaction.hasUpdatedAt())
                tx.setUpdateTime(new Date(transaction.getUpdatedAt()));
        }
        confidenceTable = getContext().getConfidenceTable();
        if (!txs.isEmpty()) {
            isHistorySynced = true;
            isChainSynced = true; //TODO refine this
        }
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
                final List<ListenableFuture<Transaction>> futures = Lists.newArrayList();
                for (AddressHistoryItem item : history) {
                    Sha256Hash hash = Sha256Hash.wrap(item.txHash);
                    if (txs.containsKey(hash) || pendingDownload.containsKey(hash))
                        continue;
                    SettableFuture<Transaction> future = addPendingDownload(hash);
                    retrieveTransaction(hash, item.height);
                    futures.add(future);
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
    SettableFuture<Transaction> addPendingDownload(Sha256Hash hash) {
        log.info("pending {}", hash);
        SettableFuture<Transaction> future = SettableFuture.create();
        SettableFuture<Transaction> existing = pendingDownload.putIfAbsent(hash, future);
        if (existing != null)
            return existing;
        return future;
    }

    @VisibleForTesting
    void retrieveTransaction(final Sha256Hash hash,
                             final int height) {
        ListenableFuture<StratumMessage> getFuture = client.call("blockchain.transaction.get", hash.toString());
        Futures.addCallback(getFuture, new FutureCallback<StratumMessage>() {
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
                pendingDownload.remove(hash).setException(t);
            }
        });
    }

    @Override
    public void onHeight(long height, Block block, boolean isSynced) {
        wallet.lock();
        try {
            this.isChainSynced = this.isChainSynced || isSynced;
            notifyHeight(height);
            while (!pendingBlock.isEmpty()) {
                TransactionWithHeight first = pendingBlock.first();
                if (first.height > height) break;
                pendingBlock.remove(first);
                Transaction tx = first.tx;
                Block txBlock = store.get(first.height);
                tx.setUpdateTime(txBlock.getTime());
                tx.getConfidence().setAppearedAtChainHeight((int)first.height);
                txs.put(tx.getHash(), tx);
                log.info("reached block for {}", tx.getHash());
                SettableFuture<Transaction> future = pendingDownload.remove(tx.getHash());
                future.set(tx);
                saveLater();
            }
        } finally {
            wallet.unlock();
        }
    }

    public int currentHeight() {
        if (store == null)
            return 0;
        return (int) store.getHeight();
    }

    @Override
    public boolean isSynced() {
        return isChainSynced && isHistorySynced;
    }

    @Override
    public List<VersionMessage> getPeers() {
        List<InetSocketAddress> addresses = client.getConnectedAddresses();
        List<VersionMessage> peers = Lists.newArrayList();
        for (InetSocketAddress address : addresses) {
            VersionMessage msg = new VersionMessage(wallet.getNetworkParameters(), (int)chain.getPeerHeight());
            msg.subVer = client.getPeerVersion();
            msg.theirAddr = new PeerAddress(address);
            peers.add(msg);
        }
        return peers;
    }

    @Override
    public List<StoredBlock> getRecentBlocks(int maxBlocks) {
        wallet.lock();
        try {
            List<StoredBlock> blocks = Lists.newArrayList();
            if (store == null)
                return Lists.newArrayList();
            long height = store.getHeight();
            while (blocks.size() < maxBlocks && height > 0) {
                Block block = store.get(height);
                if (block == null)
                    break;
                StoredBlock stored = new StoredBlock(block, BigInteger.ZERO, (int)height);
                blocks.add(stored);
                height--;
            }
            return blocks;
        } finally {
            wallet.unlock();
        }
    }

    private void resetStore() {
        checkState(store == null);
        HeadersStore temp = makeStore();
        store.truncate(RESET_HEIGHT);
        temp.close();
    }

    @Override
    public void resetBlockchain() {
        wallet.lock();
        try {
            checkState(chain == null, "must be stopped to reset");
            resetStore();
            txs.clear();
            isChainSynced = false;
            isHistorySynced = false;
            wallet.saveNow();
        } finally {
            wallet.unlock();
        }
    }

    void receive(Transaction tx, int height) {
        // This is also a workaround to Context issue at Fetcher
        TransactionConfidence confidence = tx.getConfidence(confidenceTable);

        wallet.lock();
        try {
            if (height > 0) {
                Block block = store.get(height);
                if (block == null) {
                    pendingBlock.add(new TransactionWithHeight(tx, height));
                } else {
                    log.info("have block for {}", tx.getHash());
                    pendingDownload.remove(tx.getHash()).set(tx);
                    tx.setUpdateTime(block.getTime());
                    confidence.setAppearedAtChainHeight(height);
                    txs.put(tx.getHash(), tx);
                    saveLater();
                }
            } else {
                log.info("unconfirmed {}", tx.getHash());
                pendingDownload.remove(tx.getHash()).set(tx);
                txs.put(tx.getHash(), tx);
                saveLater();
            }
        } finally {
            wallet.unlock();
        }
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

    private void notifyHeight(final long height) {
        final boolean isSynced = isChainSynced && isHistorySynced;
        log.info("notify height {} {} {}", isChainSynced, isHistorySynced, height);
        for (final ListenerRegistration<MultiWalletEventListener> registration : eventListeners) {
            if (registration.executor == Threading.SAME_THREAD) {
                registration.listener.onSyncState(this, isSynced, height);
            } else {
                registration.executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        registration.listener.onSyncState(ElectrumMultiWallet.this, isSynced, height);
                    }
                });
            }
        }
    }
}
