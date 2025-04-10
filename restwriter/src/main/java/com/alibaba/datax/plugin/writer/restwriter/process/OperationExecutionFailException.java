package com.alibaba.datax.plugin.writer.restwriter.process;

/**
 * @name: zhangyongxiang
 * @author: zhangyongxiang@baidu.com
 **/

public class OperationExecutionFailException extends RuntimeException {
    
    private static final long serialVersionUID = 2848134252562605007L;
    
    public OperationExecutionFailException(final String message,
            final Throwable cause) {
        super(message, cause);
    }
}
