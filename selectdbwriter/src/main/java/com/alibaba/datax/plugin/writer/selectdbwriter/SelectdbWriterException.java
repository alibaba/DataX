package com.alibaba.datax.plugin.writer.selectdbwriter;


public class SelectdbWriterException extends RuntimeException  {

    private boolean reCreateLabel;


    public SelectdbWriterException() {
        super();
    }

    public SelectdbWriterException(String message) {
        super(message);
    }

    public SelectdbWriterException(String message, boolean reCreateLabel) {
        super(message);
        this.reCreateLabel = reCreateLabel;
    }

    public SelectdbWriterException(String message, Throwable cause) {
        super(message, cause);
    }

    public SelectdbWriterException(Throwable cause) {
        super(cause);
    }

    protected SelectdbWriterException(String message, Throwable cause,
                                      boolean enableSuppression,
                                      boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public boolean needReCreateLabel() {
        return reCreateLabel;
    }
}
