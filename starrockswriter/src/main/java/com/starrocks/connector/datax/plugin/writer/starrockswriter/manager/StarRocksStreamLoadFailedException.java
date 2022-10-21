package com.starrocks.connector.datax.plugin.writer.starrockswriter.manager;

import java.io.IOException;
import java.util.Map;

 
public class StarRocksStreamLoadFailedException extends IOException {

    static final long serialVersionUID = 1L;

    private final Map<String, Object> response;
    private boolean reCreateLabel;

    public StarRocksStreamLoadFailedException(String message, Map<String, Object> response) {
        super(message);
        this.response = response;
    }

    public StarRocksStreamLoadFailedException(String message, Map<String, Object> response, boolean reCreateLabel) {
        super(message);
        this.response = response;
        this.reCreateLabel = reCreateLabel;
    }

    public Map<String, Object> getFailedResponse() {
        return response;
    }

    public boolean needReCreateLabel() {
        return reCreateLabel;
    }

}
