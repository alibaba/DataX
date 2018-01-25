package com.alibaba.datax.plugin.reader.otsstreamreader.internal;

import com.alibaba.datax.common.spi.ErrorCode;

public class OTSReaderError implements ErrorCode {

    private String code;

    private String description;

    public final static OTSReaderError ERROR = new OTSReaderError("OTSStreamReaderError", "OTS Stream Reader Error");

    public final static OTSReaderError INVALID_PARAM = new OTSReaderError(
        "OTSStreamReaderInvalidParameter", "OTS Stream Reader Invalid Parameter");

    public OTSReaderError(String code, String description) {
        this.code = code;
        this.description = description;
    }

    public String getCode() {
        return this.code;
    }

    public String getDescription() {
        return this.description;
    }

    public String toString() {
        return "[ code:" + this.code + ", message" + this.description + "]";
    }
}
