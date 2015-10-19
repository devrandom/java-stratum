package org.smartwallet.multi;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import org.bitcoinj.core.*;
import org.bitcoinj.script.Script;
import org.bitcoinj.utils.Threading;
import org.bitcoinj.wallet.KeyChainGroup;
import org.bitcoinj.wallet.WalletTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartcolors.SmartWallet;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * Wrap a normal BitcoinJ SPV wallet
 * 
 * Created by devrandom on 2015-09-08.
 */
public class SPVMultiWallet extends SmartMultiWallet {
    protected static final Logger log = LoggerFactory.getLogger(SPVMultiWallet.class);

    protected final PeerGroup peers;
    private final Map<MultiWalletEventListener, WalletEventListener> listenerMap;

    public SPVMultiWallet(SmartWallet wallet, PeerGroup peers) {
        super(wallet);
        this.peers = peers;
        listenerMap = Maps.newConcurrentMap();
    }

    @Override
    public void addEventListener(final MultiWalletEventListener listener, Executor executor) {
        AbstractWalletEventListener walletListener = new AbstractWalletEventListener() {
            @Override
            public void onCoinsReceived(Wallet wallet, Transaction tx, Coin prevBalance, Coin newBalance) {
                super.onCoinsReceived(wallet, tx, prevBalance, newBalance);
                listener.onTransaction(SPVMultiWallet.this, tx);
            }

            @Override
            public void onCoinsSent(Wallet wallet, Transaction tx, Coin prevBalance, Coin newBalance) {
                // FIXME change bitcoinj so that we get this also when diff = 0
                super.onCoinsSent(wallet, tx, prevBalance, newBalance);
                listener.onTransaction(SPVMultiWallet.this, tx);
            }

            @Override
            public void onTransactionConfidenceChanged(Wallet wallet, Transaction tx) {
                super.onTransactionConfidenceChanged(wallet, tx);
            }
        };
        listenerMap.put(listener, walletListener);
        wallet.addEventListener(walletListener, Threading.SAME_THREAD);
    }

    @Override
    public boolean removeEventListener(MultiWalletEventListener listener) {
        return wallet.removeEventListener(listenerMap.remove(listener));
    }

    @Override
    public Set<Transaction> getTransactions() {
        return wallet.getTransactions(true);
    }

    @Override
    public Map<Sha256Hash, Transaction> getTransactionPool(WalletTransaction.Pool pool) {
        return wallet.getTransactionPool(pool);
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
                    log.warn("Could not parse tx output script: {}", e.toString());
                }
            }
        } finally {
            wallet.unlockKeychain();
        }
    }

    @Override
    public void completeTx(Wallet.SendRequest req) throws InsufficientMoneyException {
        wallet.completeTx(req);
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
    public ListenableFuture<Transaction> broadcastTransaction(Transaction tx) {
        return peers.broadcastTransaction(tx).future();
    }

    @Override
    public void start() {
        peers.start();
    }

    @Override
    public void startAsync() {
        peers.startAsync();
    }

    @Override
    public void awaitDownload() throws InterruptedException {
        DownloadProgressTracker listener = new DownloadProgressTracker();
        peers.startBlockChainDownload(listener);
        listener.await();
    }
}
