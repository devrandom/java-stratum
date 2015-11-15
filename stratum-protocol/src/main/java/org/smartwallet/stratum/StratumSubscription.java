package org.smartwallet.stratum;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Created by devrandom on 2015-Sep-04.
 */
public class StratumSubscription {
    public final ListenableFuture<StratumMessage> future;

    public StratumSubscription(ListenableFuture<StratumMessage> future) {
        this.future = future;
    }
}
