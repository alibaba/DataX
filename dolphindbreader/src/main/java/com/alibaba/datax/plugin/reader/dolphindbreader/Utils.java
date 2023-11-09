package com.alibaba.datax.plugin.reader.dolphindbreader;

import com.alibaba.datax.common.util.Configuration;
import com.xxdb.DBConnection;

public class Utils {

    protected static DBConnection connectDB(Configuration readerConfig) throws Exception{
        DBConnection dbConnection = new DBConnection();
        dbConnection.connect(
                readerConfig.getString(Key.HOST),
                readerConfig.getInt(Key.PORT),
                readerConfig.getString(Key.USER_ID),
                readerConfig.getString(Key.PWD)
        );
        return dbConnection;
    }

    protected static void validateParameter(Configuration readerConfig) {
        readerConfig.getNecessaryValue(Key.HOST, DolphinDbWriterErrorCode.REQUIRED_VALUE);
        readerConfig.getNecessaryValue(Key.PORT, DolphinDbWriterErrorCode.REQUIRED_VALUE);
        readerConfig.getNecessaryValue(Key.PWD, DolphinDbWriterErrorCode.REQUIRED_VALUE);
        readerConfig.getNecessaryValue(Key.USER_ID, DolphinDbWriterErrorCode.REQUIRED_VALUE);
    }

}
