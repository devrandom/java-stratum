package org.smartwallet.multi;

import org.bitcoinj.core.*;
import org.bitcoinj.wallet.WalletTransaction;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * Wrap some functions of BitcoinJ's {@link Wallet} that can be implemented in a different way in non-SPV
 * situations.
 * 
 * Created by devrandom on 2015-09-08.
 */
public interface MultiWallet extends TransactionBag {
    void addEventListener(WalletEventListener listener, Executor executor);
    boolean removeEventListener(WalletEventListener listener);
    Set<Transaction> getTransactions();
    Map<Sha256Hash, Transaction> getTransactionPool(WalletTransaction.Pool pool);
    void markKeysAsUsed(Transaction tx);
    void completeTx(Wallet.SendRequest req) throws InsufficientMoneyException;
    void commitTx(Transaction tx);
    List<TransactionOutput> calculateAllSpendCandidates(boolean excludeImmatureCoinbases, boolean excludeUnsignable);
    ListenableFuture<Transaction> broadcastTransaction(final Transaction tx);

    /** Start the network layer and wait for it to come up */
    void start();

    /** Start the network layer */
    void startAsync();

    /** must be called after start or startAsync */
    void awaitDownload() throws InterruptedException;
}
