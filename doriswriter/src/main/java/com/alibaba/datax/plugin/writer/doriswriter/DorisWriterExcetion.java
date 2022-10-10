package com.alibaba.datax.plugin.writer.doriswriter;

import java.io.IOException;
import java.util.Map;

public class DorisWriterExcetion extends IOException {

    private final Map<String, Object> response;
    private boolean reCreateLabel;

    public DorisWriterExcetion ( String message, Map<String, Object> response) {
        super(message);
        this.response = response;
    }

    public DorisWriterExcetion ( String message, Map<String, Object> response, boolean reCreateLabel) {
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
