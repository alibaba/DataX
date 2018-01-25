package com.alibaba.datax.plugin.writer.adswriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum AdsWriterErrorCode implements ErrorCode {
    REQUIRED_VALUE("AdsWriter-00", "您缺失了必须填写的参数值."),
    NO_ADS_TABLE("AdsWriter-01", "ADS表不存在."),
    ODPS_CREATETABLE_FAILED("AdsWriter-02", "创建ODPS临时表失败，请联系ADS 技术支持"),
    ADS_LOAD_TEMP_ODPS_FAILED("AdsWriter-03", "ADS从ODPS临时表导数据失败，请联系ADS 技术支持"),
    TABLE_TRUNCATE_ERROR("AdsWriter-04", "清空 ODPS 目的表时出错."),
    CREATE_ADS_HELPER_FAILED("AdsWriter-05", "创建ADSHelper对象出错，请联系ADS 技术支持"),
    ODPS_PARTITION_FAILED("AdsWriter-06", "ODPS Reader不允许配置多个partition，目前只支持三种配置方式，\"partition\":[\"pt=*,ds=*\"](读取test表所有分区的数据)； \n" +
            "\"partition\":[\"pt=1,ds=*\"](读取test表下面，一级分区pt=1下面的所有二级分区)； \n" +
            "\"partition\":[\"pt=1,ds=hangzhou\"](读取test表下面，一级分区pt=1下面，二级分区ds=hz的数据)"),
    ADS_LOAD_ODPS_FAILED("AdsWriter-07", "ADS从ODPS导数据失败，请联系ADS 技术支持，先检查ADS账号是否已加到该ODPS Project中。ADS账号为："),
    INVALID_CONFIG_VALUE("AdsWriter-08", "不合法的配置值."),

    GET_ADS_TABLE_MEATA_FAILED("AdsWriter-11", "获取ADS table原信息失败");

    private final String code;
    private final String description;
    private String adsAccount;


    private AdsWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    public void setAdsAccount(String adsAccount) {
        this.adsAccount = adsAccount;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        if (this.code.equals("AdsWriter-07")){
            return String.format("Code:[%s], Description:[%s][%s]. ", this.code,
                    this.description,adsAccount);
        }else{
            return String.format("Code:[%s], Description:[%s]. ", this.code,
                    this.description);
        }
    }
}
