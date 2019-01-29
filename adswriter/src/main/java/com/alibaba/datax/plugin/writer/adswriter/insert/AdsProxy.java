package com.alibaba.datax.plugin.writer.adswriter.insert;

import com.alibaba.datax.common.plugin.RecordReceiver;

import java.sql.Connection;

public interface AdsProxy {
    public abstract void startWriteWithConnection(RecordReceiver recordReceiver, Connection connection,
                                                  int columnNumber);

    public void closeResource();
}
