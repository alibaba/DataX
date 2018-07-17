package com.alibaba.datax.plugin.writer.adswriter;

public class AdsException extends Exception {

    private static final long serialVersionUID = 1080618043484079794L;

    public final static int ADS_CONN_URL_NOT_SET = -100;
    public final static int ADS_CONN_USERNAME_NOT_SET = -101;
    public final static int ADS_CONN_PASSWORD_NOT_SET = -102;
    public final static int ADS_CONN_SCHEMA_NOT_SET = -103;

    public final static int JOB_NOT_EXIST = -200;
    public final static int JOB_FAILED = -201;

    public final static int ADS_LOADDATA_SCHEMA_NULL = -300;
    public final static int ADS_LOADDATA_TABLE_NULL = -301;
    public final static int ADS_LOADDATA_SOURCEPATH_NULL = -302;
    public final static int ADS_LOADDATA_JOBID_NOT_AVAIL = -303;
    public final static int ADS_LOADDATA_FAILED = -304;

    public final static int ADS_TABLEMETA_SCHEMA_NULL = -404;
    public final static int ADS_TABLEMETA_TABLE_NULL = -405;

    public final static int OTHER = -999;

    private int code = OTHER;
    private String message;

    public AdsException(int code, String message, Throwable e) {
        super(message, e);
        this.code = code;
        this.message = message;
    }

    @Override
    public String getMessage() {
        return "Code=" + this.code + " Message=" + this.message;
    }

}
