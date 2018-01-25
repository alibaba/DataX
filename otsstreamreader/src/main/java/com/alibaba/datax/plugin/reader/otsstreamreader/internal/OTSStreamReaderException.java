package com.alibaba.datax.plugin.reader.otsstreamreader.internal;

public class OTSStreamReaderException extends RuntimeException {

    public OTSStreamReaderException(String message) {
        super(message);
    }

    public OTSStreamReaderException(String message, Exception cause) {
        super(message, cause);
    }
}
