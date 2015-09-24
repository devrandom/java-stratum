package org.smartwallet.stratum;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created by devrandom on 2015-Aug-25.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class StratumMessage {
    public Long id;

    /** RPC method - for calls */
    public String method;

    /** Parameters - for calls */
    public List<JsonNode> params;

    /** Result - for result */
    @JsonProperty("result")
    public JsonNode result;

    @JsonProperty("error")
    public String error;

    public static final StratumMessage SENTINEL = new StratumMessage();

    public StratumMessage() {
    }

    @JsonIgnore
    public StratumMessage(Long id, String method, List<Object> params, ObjectMapper mapper) {
        this.id = id;
        this.method = method;
        this.params = Lists.newArrayList();
        for (Object param : params) {
            this.params.add(mapper.valueToTree(param));
        }
    }

    @JsonIgnore
    public StratumMessage(Long id, JsonNode result) {
        this.id = id;
        this.result = result;
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

    @JsonIgnore
    public boolean isError() {
        return id != null && error != null;
    }
}
