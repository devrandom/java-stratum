package org.smartwallet.stratum;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.concurrent.BlockingQueue;

/**
 * Created by devrandom on 2015-Sep-04.
 */
public class StratumSubscription {
    public final ListenableFuture<StratumMessage> future;
    public final BlockingQueue<StratumMessage> queue;

    public StratumSubscription(ListenableFuture<StratumMessage> future, BlockingQueue<StratumMessage> queue) {
        this.future = future;
        this.queue = queue;
    }
}
