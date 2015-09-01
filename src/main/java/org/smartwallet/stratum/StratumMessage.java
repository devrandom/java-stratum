package org.smartwallet.stratum;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Created by devrandom on 2015-Aug-25.
 */
public class StratumMessage {
    public Long id;

    /** RPC method - for calls */
    public String method;

    /** Parameters - for calls */
    public List<Object> params;

    /** Result - for result */
    @JsonProperty("result")
    public Object result;

    public static final StratumMessage SENTINEL = new StratumMessage(null, null, null);

    public StratumMessage() {
    }

    @JsonIgnore
    public StratumMessage(Long id, String method, List<Object> params) {
        this.id = id;
        this.method = method;
        this.params = params;
    }

    @JsonIgnore
    public boolean isResult() {
        return id != null && result != null;
    }

    @JsonIgnore
    public boolean isMessage() {
        return id == null && method != null && params != null;
    }

    @JsonIgnore
    public boolean isSentinel() {
        return this == SENTINEL;
    }
}
