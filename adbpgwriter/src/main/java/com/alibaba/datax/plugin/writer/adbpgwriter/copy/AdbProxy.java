package com.alibaba.datax.plugin.writer.adbpgwriter.copy;

import com.alibaba.datax.common.plugin.RecordReceiver;

import java.sql.Connection;
/**
 * @author yuncheng
 */
public interface AdbProxy {
    public abstract void startWriteWithConnection(RecordReceiver recordReceiver, Connection connection);

    public void closeResource();
}
