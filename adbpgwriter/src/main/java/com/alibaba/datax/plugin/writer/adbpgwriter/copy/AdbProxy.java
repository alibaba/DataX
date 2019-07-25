package com.alibaba.datax.plugin.writer.adbpgwriter.copy;

import com.alibaba.datax.common.plugin.RecordReceiver;

import java.sql.Connection;
/**
 * Created by yuncheng on 07/15/2019.
 */
public interface AdbProxy {
    public abstract void startWriteWithConnection(RecordReceiver recordReceiver, Connection connection);

    public void closeResource();
}
