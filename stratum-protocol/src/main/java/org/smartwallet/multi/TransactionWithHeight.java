package org.smartwallet.multi;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import org.bitcoinj.core.Transaction;

/**
 * Created by devrandom on 2015-Nov-08.
 */
public class TransactionWithHeight implements Comparable<TransactionWithHeight> {
    public Transaction tx;
    public long height;

    public TransactionWithHeight(Transaction tx, int height) {
        this.tx = tx;
        this.height = height;
    }

    @Override
    public int compareTo(TransactionWithHeight o) {
        return ComparisonChain.start()
                .compare(height, o.height)
                .compare(tx.getHash(), o.tx.getHash())
                .result();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TransactionWithHeight))
            return false;
        TransactionWithHeight o = (TransactionWithHeight) obj;
        return o.tx.equals(tx);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(tx, height);
    }
}
