package com.alibaba.datax.plugin.writer;

import java.util.Properties;

public class JniConnection {

    private static final long JNI_NULL_POINTER = 0L;
    private static final String PROPERTY_KEY_CONFIG_DIR = "cfgdir";
    private static final String PROPERTY_KEY_LOCALE = "locale";
    private static final String PROPERTY_KEY_CHARSET = "charset";
    private static final String PROPERTY_KEY_TIME_ZONE = "timezone";

    private long psql;

    static {
        System.loadLibrary("taos");
    }

    public JniConnection(Properties props) {
        if (this.psql != JNI_NULL_POINTER) {
            close();
            this.psql = JNI_NULL_POINTER;
        }

        initImp(props.getProperty(PROPERTY_KEY_CONFIG_DIR, null));

        String locale = props.getProperty(PROPERTY_KEY_LOCALE);
        if (setOptions(0, locale) < 0) {
            throw new RuntimeException("Failed to set locale: " + locale + ". System default will be used.");
        }
        String charset = props.getProperty(PROPERTY_KEY_CHARSET);
        if (setOptions(1, charset) < 0) {
            throw new RuntimeException("Failed to set charset: " + charset + ". System default will be used.");
        }
        String timezone = props.getProperty(PROPERTY_KEY_TIME_ZONE);
        if (setOptions(2, timezone) < 0) {
            throw new RuntimeException("Failed to set timezone: " + timezone + ". System default will be used.");
        }
    }

    public void open(String host, int port, String dbname, String user, String password) {
        if (this.psql != JNI_NULL_POINTER) {
            close();
            this.psql = JNI_NULL_POINTER;
        }

        this.psql = connectImp(host, port, dbname, user, password);
        if (this.psql == JNI_NULL_POINTER) {
            String errMsg = getErrMsgImp(0);
            throw new RuntimeException(errMsg);
        }
    }

    public long insertOpentsdbJson(String json) {
        if (this.psql == JNI_NULL_POINTER) {
            throw new RuntimeException("JNI connection is NULL");
        }
        return insertOpentsdbJson(json, this.psql);
    }

    public void close() {
        int code = this.closeConnectionImp(this.psql);
        if (code != 0) {
            throw new RuntimeException("JNI closeConnection failed");
        }
        this.psql = JNI_NULL_POINTER;
    }

    private static native void initImp(String configDir);

    private static native int setOptions(int optionIndex, String optionValue);

    private static native String getTsCharset();

    private native long connectImp(String host, int port, String dbName, String user, String password);

    private native long executeQueryImp(byte[] sqlBytes, long connection);

    private native int getErrCodeImp(long connection, long pSql);

    private native String getErrMsgImp(long pSql);

    private native int getAffectedRowsImp(long connection, long pSql);

    private native int closeConnectionImp(long connection);

    private native long insertOpentsdbJson(String json, long pSql);

}
