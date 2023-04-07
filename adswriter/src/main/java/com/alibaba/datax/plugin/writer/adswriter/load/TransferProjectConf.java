package com.alibaba.datax.plugin.writer.adswriter.load;

import com.alibaba.datax.common.util.Configuration;

/**
 * Created by xiafei.qiuxf on 15/4/13.
 */
public class TransferProjectConf {

    public final static String KEY_ACCESS_ID = "odps.accessId";
    public final static String KEY_ACCESS_KEY = "odps.accessKey";
    public final static String KEY_ACCOUNT = "odps.account";
    public final static String KEY_ODPS_SERVER = "odps.odpsServer";
    public final static String KEY_ODPS_TUNNEL = "odps.tunnelServer";
    public final static String KEY_PROJECT = "odps.project";

    private String accessId;
    private String accessKey;
    private String account;
    private String odpsServer;
    private String odpsTunnel;
    private String project;

    public static  TransferProjectConf create(Configuration adsWriterConf) {
        TransferProjectConf res = new TransferProjectConf();
        res.accessId = adsWriterConf.getString(KEY_ACCESS_ID);
        res.accessKey = adsWriterConf.getString(KEY_ACCESS_KEY);
        res.account = adsWriterConf.getString(KEY_ACCOUNT);
        res.odpsServer = adsWriterConf.getString(KEY_ODPS_SERVER);
        res.odpsTunnel = adsWriterConf.getString(KEY_ODPS_TUNNEL);
        res.project = adsWriterConf.getString(KEY_PROJECT);
        return res;
    }

    public String getAccessId() {
        return accessId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getAccount() {
        return account;
    }

    public String getOdpsServer() {
        return odpsServer;
    }

    public String getOdpsTunnel() {
        return odpsTunnel;
    }


    public String getProject() {
        return project;
    }
}
