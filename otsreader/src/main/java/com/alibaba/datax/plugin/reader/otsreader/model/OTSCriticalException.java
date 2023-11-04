package com.alibaba.datax.plugin.reader.otsreader.model;

/**
 * 插件错误异常，该异常主要用于描述插件的异常退出
 * @author redchen
 */
public class OTSCriticalException extends Exception{

    private static final long serialVersionUID = 5820460098894295722L;
    
    public OTSCriticalException() {}
    
    public OTSCriticalException(String message) {
        super(message);
    }
    
    public OTSCriticalException(Throwable a) {
        super(a);
    }
    
    public OTSCriticalException(String message, Throwable a) {
        super(message, a);
    }
}
