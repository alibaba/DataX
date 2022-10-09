package com.alibaba.datax.plugin.writer.doriswriter;

import java.io.IOException;
import java.util.Map;

public class DorisStreamLoadExcetion extends IOException {

    private final Map<String, Object> response;
    private boolean reCreateLabel;

    public DorisStreamLoadExcetion(String message, Map<String, Object> response) {
        super(message);
        this.response = response;
    }

    public DorisStreamLoadExcetion(String message, Map<String, Object> response, boolean reCreateLabel) {
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
