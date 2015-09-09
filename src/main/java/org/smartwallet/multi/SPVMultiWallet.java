package org.smartwallet.multi;

import org.bitcoinj.core.*;
import org.bitcoinj.script.Script;
import org.bitcoinj.wallet.KeyChainGroup;
import org.bitcoinj.wallet.WalletTransaction;

import com.google.common.util.concurrent.ListenableFuture;
import org.smartwallet.stratum.StratumMessage;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * Wrap a normal BitcoinJ SPV wallet
 * 
 * Created by devrandom on 2015-09-08.
 */
public class SPVMultiWallet implements MultiWallet {
    protected final SmartWallet wallet;
    protected final PeerGroup peers;

    public SPVMultiWallet(SmartWallet wallet, PeerGroup peers) {
        this.wallet = wallet;
        this.peers = peers;
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
        return wallet.getTransactions(true);
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
                    SmartWallet.log.warn("Could not parse tx output script: {}", e.toString());
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
}
