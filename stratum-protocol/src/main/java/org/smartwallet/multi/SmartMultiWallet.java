package org.smartwallet.multi;

import org.bitcoinj.core.*;
import org.bitcoinj.script.Script;
import org.smartcolors.MultiWallet;
import org.smartcolors.SmartWallet;

import java.util.List;

/**
 * Created by devrandom on 2015-Oct-19.
 */
abstract public class SmartMultiWallet implements MultiWallet {
    protected final SmartWallet wallet;

    public SmartMultiWallet(SmartWallet wallet) {
        this.wallet = wallet;
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
    public void lock() {
        wallet.lock();
    }

    @Override
    public void unlock() {
        wallet.unlock();
    }

    @Override
    public List<TransactionOutput> getWalletOutputs(Transaction tx) {
        return tx.getWalletOutputs(wallet);
    }

    @Override
    public Transaction getTransaction(Sha256Hash hash) {
        return wallet.getTransaction(hash);
    }

    @Override
    public void saveLater() {
        wallet.doSaveLater();
    }

    @Override
    public Context getContext() {
        return wallet.getContext();
    }
}
