package com.alibaba.datax.plugin.writer.hbase094xwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;


/**
 * Created by shf on 16/3/7.
 */
public class Hbase094xHelper {

    private static final Logger LOG = LoggerFactory.getLogger(Hbase094xHelper.class);

    /**
     *
     * @param hbaseConfig
     * @return
     */
    public static org.apache.hadoop.conf.Configuration getHbaseConfiguration(String hbaseConfig) {
        if (StringUtils.isBlank(hbaseConfig)) {
            throw DataXException.asDataXException(Hbase094xWriterErrorCode.REQUIRED_VALUE, "读 Hbase 时需要配置hbaseConfig，其内容为 Hbase 连接信息，请联系 Hbase PE 获取该信息.");
        }
        org.apache.hadoop.conf.Configuration hConfiguration = HBaseConfiguration.create();
        try {
            Map<String, String> hbaseConfigMap = JSON.parseObject(hbaseConfig, new TypeReference<Map<String, String>>() {});
            // 用户配置的 key-value 对 来表示 hbaseConfig
            Validate.isTrue(hbaseConfigMap != null, "hbaseConfig不能为空Map结构!");
            for (Map.Entry<String, String> entry : hbaseConfigMap.entrySet()) {
                hConfiguration.set(entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(Hbase094xWriterErrorCode.GET_HBASE_CONFIG_ERROR, e);
        }
        return hConfiguration;
    }


    public static HTable getTable(com.alibaba.datax.common.util.Configuration configuration){
        String hbaseConfig = configuration.getString(Key.HBASE_CONFIG);
        String userTable = configuration.getString(Key.TABLE);
        org.apache.hadoop.conf.Configuration hConfiguration = Hbase094xHelper.getHbaseConfiguration(hbaseConfig);
        Boolean autoFlush = configuration.getBool(Key.AUTO_FLUSH, false);
        long writeBufferSize = configuration.getLong(Key.WRITE_BUFFER_SIZE, Constant.DEFAULT_WRITE_BUFFER_SIZE);

        HTable htable = null;
        HBaseAdmin admin = null;
        try {
            htable = new HTable(hConfiguration, userTable);
            admin = new HBaseAdmin(hConfiguration);
            Hbase094xHelper.checkHbaseTable(admin,htable);
            //本期设置autoflush 一定为flase,通过hbase writeBufferSize来控制每次flush大小
            htable.setAutoFlush(false);
            htable.setWriteBufferSize(writeBufferSize);
            return htable;
        } catch (Exception e) {
            Hbase094xHelper.closeTable(htable);
            throw DataXException.asDataXException(Hbase094xWriterErrorCode.GET_HBASE_TABLE_ERROR, e);
        }finally {
            Hbase094xHelper.closeAdmin(admin);
        }
    }

    public static void deleteTable(com.alibaba.datax.common.util.Configuration configuration) {
        String userTable = configuration.getString(Key.TABLE);
        LOG.info(String.format("由于您配置了deleteType delete,HBasWriter begins to delete table %s .", userTable));
        Scan scan = new Scan();
        HTable hTable =Hbase094xHelper.getTable(configuration);
        ResultScanner scanner = null;
        try {
            scanner = hTable.getScanner(scan);
            for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                hTable.delete(new Delete(rr.getRow()));
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(Hbase094xWriterErrorCode.DELETE_HBASE_ERROR, e);
        }finally {
            if(scanner != null){
                scanner.close();
            }
            Hbase094xHelper.closeTable(hTable);
        }
    }

    public static void truncateTable(com.alibaba.datax.common.util.Configuration configuration)  {

        String hbaseConfig = configuration.getString(Key.HBASE_CONFIG);
        String userTable = configuration.getString(Key.TABLE);
        org.apache.hadoop.conf.Configuration hConfiguration = Hbase094xHelper.getHbaseConfiguration(hbaseConfig);

        HTable htable = null;
        HBaseAdmin admin = null;
        LOG.info(String.format("由于您配置了deleteType truncate,HBasWriter begins to truncate table %s .", userTable));
        try{
            htable = new HTable(hConfiguration, userTable);
            admin = new HBaseAdmin(hConfiguration);
            HTableDescriptor descriptor  = htable.getTableDescriptor();
            Hbase094xHelper.checkHbaseTable(admin,htable);
            admin.disableTable(htable.getTableName());
            admin.deleteTable(htable.getTableName());
            admin.createTable(descriptor);
        }catch (Exception e) {
            throw DataXException.asDataXException(Hbase094xWriterErrorCode.TRUNCATE_HBASE_ERROR, e);
        }finally {
            Hbase094xHelper.closeAdmin(admin);
            Hbase094xHelper.closeTable(htable);
        }
    }



    public static void closeAdmin(HBaseAdmin admin){
        try {
            if(null != admin)
                admin.close();
        } catch (IOException e) {
            throw DataXException.asDataXException(Hbase094xWriterErrorCode.CLOSE_HBASE_AMIN_ERROR, e);
        }
    }

    public static void closeTable(HTable table){
        try {
            if(null != table)
                table.close();
        } catch (IOException e) {
            throw DataXException.asDataXException(Hbase094xWriterErrorCode.CLOSE_HBASE_TABLE_ERROR, e);
        }
    }


    public static  void checkHbaseTable(HBaseAdmin admin,  HTable hTable) throws IOException {
        if (!admin.isMasterRunning()) {
            throw new IllegalStateException("HBase master 没有运行, 请检查您的配置 或者 联系 Hbase 管理员.");
        }
        if (!admin.tableExists(hTable.getTableName())) {
            throw new IllegalStateException("HBase源头表" + Bytes.toString(hTable.getTableName())
                    + "不存在, 请检查您的配置 或者 联系 Hbase 管理员.");
        }
        if (!admin.isTableAvailable(hTable.getTableName()) || !admin.isTableEnabled(hTable.getTableName())) {
            throw new IllegalStateException("HBase源头表" + Bytes.toString(hTable.getTableName())
                    + " 不可用, 请检查您的配置 或者 联系 Hbase 管理员.");
        }
        if(admin.isTableDisabled(hTable.getTableName())){
            throw new IllegalStateException("HBase源头表" + Bytes.toString(hTable.getTableName())
                    + " 不可用, 请检查您的配置 或者 联系 Hbase 管理员.");
        }
    }


    public static void validateParameter(com.alibaba.datax.common.util.Configuration originalConfig) {
        originalConfig.getNecessaryValue(Key.HBASE_CONFIG, Hbase094xWriterErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.TABLE, Hbase094xWriterErrorCode.REQUIRED_VALUE);

        Hbase094xHelper.validateMode(originalConfig);

        String encoding = originalConfig.getString(Key.ENCODING, Constant.DEFAULT_ENCODING);
        if (!Charset.isSupported(encoding)) {
            throw DataXException.asDataXException(Hbase094xWriterErrorCode.ILLEGAL_VALUE, String.format("Hbasewriter 不支持您所配置的编码:[%s]", encoding));
        }
        originalConfig.set(Key.ENCODING, encoding);
        Boolean autoFlush = originalConfig.getBool(Key.AUTO_FLUSH, false);
        //本期设置autoflush 一定为flase,通过hbase writeBufferSize来控制每次flush大小
        originalConfig.set(Key.AUTO_FLUSH,false);
        Boolean walFlag = originalConfig.getBool(Key.WAL_FLAG, false);
        originalConfig.set(Key.WAL_FLAG, walFlag);
        long writeBufferSize = originalConfig.getLong(Key.WRITE_BUFFER_SIZE,Constant.DEFAULT_WRITE_BUFFER_SIZE);
        originalConfig.set(Key.WRITE_BUFFER_SIZE, writeBufferSize);
    }




    public  static  void validateMode(com.alibaba.datax.common.util.Configuration originalConfig){
        String mode = originalConfig.getNecessaryValue(Key.MODE, Hbase094xWriterErrorCode.REQUIRED_VALUE);
        ModeType modeType = ModeType.getByTypeName(mode);
        switch (modeType) {
            case Normal: {
                validateRowkeyColumn(originalConfig);
                validateColumn(originalConfig);
                validateVersionColumn(originalConfig);
                break;
            }
            default:
                throw DataXException.asDataXException(Hbase094xWriterErrorCode.ILLEGAL_VALUE,
                        String.format("Hbase11xWriter不支持该 mode 类型:%s", mode));
        }
    }

    public static void validateColumn(com.alibaba.datax.common.util.Configuration originalConfig){
        List<Configuration> columns = originalConfig.getListConfiguration(Key.COLUMN);
        if (columns == null || columns.isEmpty()) {
            throw DataXException.asDataXException(Hbase094xWriterErrorCode.REQUIRED_VALUE, "column为必填项，其形式为：column:[{\"index\": 0,\"name\": \"cf0:column0\",\"type\": \"string\"},{\"index\": 1,\"name\": \"cf1:column1\",\"type\": \"long\"}]");
        }
        for (Configuration aColumn : columns) {
            Integer index = aColumn.getInt(Key.INDEX);
            String type = aColumn.getNecessaryValue(Key.TYPE, Hbase094xWriterErrorCode.REQUIRED_VALUE);
            String name = aColumn.getNecessaryValue(Key.NAME, Hbase094xWriterErrorCode.REQUIRED_VALUE);
            ColumnType.getByTypeName(type);
            if(name.split(":").length != 2){
                throw DataXException.asDataXException(Hbase094xWriterErrorCode.ILLEGAL_VALUE, String.format("您column配置项中name配置的列格式[%s]不正确，name应该配置为 列族:列名  的形式, 如 {\"index\": 1,\"name\": \"cf1:q1\",\"type\": \"long\"}", name));
            }
            if(index == null || index < 0){
                throw DataXException.asDataXException(Hbase094xWriterErrorCode.ILLEGAL_VALUE, "您的column配置项不正确,配置项中中index为必填项,且为非负数，请检查并修改.");
            }
        }
    }

    public static void validateRowkeyColumn(com.alibaba.datax.common.util.Configuration originalConfig){
        List<Configuration> rowkeyColumn = originalConfig.getListConfiguration(Key.ROWKEY_COLUMN);
        if (rowkeyColumn == null || rowkeyColumn.isEmpty()) {
            throw DataXException.asDataXException(Hbase094xWriterErrorCode.REQUIRED_VALUE, "rowkeyColumn为必填项，其形式为：rowkeyColumn:[{\"index\": 0,\"type\": \"string\"},{\"index\": -1,\"type\": \"string\",\"value\": \"_\"}]");
        }
        int rowkeyColumnSize = rowkeyColumn.size();
        //包含{"index":0,"type":"string"} 或者 {"index":-1,"type":"string","value":"_"}
        for (Configuration aRowkeyColumn : rowkeyColumn) {
            Integer index = aRowkeyColumn.getInt(Key.INDEX);
            String type = aRowkeyColumn.getNecessaryValue(Key.TYPE, Hbase094xWriterErrorCode.REQUIRED_VALUE);
            ColumnType.getByTypeName(type);
            if(index == null ){
                throw DataXException.asDataXException(Hbase094xWriterErrorCode.REQUIRED_VALUE, "rowkeyColumn配置项中index为必填项");
            }
            //不能只有-1列,即rowkey连接串
            if(rowkeyColumnSize ==1 && index == -1){
                throw DataXException.asDataXException(Hbase094xWriterErrorCode.ILLEGAL_VALUE, "rowkeyColumn配置项不能全为常量列,至少指定一个rowkey列");
            }
            if(index == -1){
                aRowkeyColumn.getNecessaryValue(Key.VALUE, Hbase094xWriterErrorCode.REQUIRED_VALUE);
            }
        }
    }

    public static void validateVersionColumn(com.alibaba.datax.common.util.Configuration originalConfig){
        Configuration versionColumn = originalConfig.getConfiguration(Key.VERSION_COLUMN);
        //为null,表示用当前时间;指定列,需要index
        if(versionColumn != null){
            Integer index = versionColumn.getInt(Key.INDEX);
            if(index == null ){
                throw DataXException.asDataXException(Hbase094xWriterErrorCode.REQUIRED_VALUE, "versionColumn配置项中index为必填项");
            }
            if(index == -1){
                //指定时间,需要index=-1,value
                versionColumn.getNecessaryValue(Key.VALUE, Hbase094xWriterErrorCode.REQUIRED_VALUE);
            }else if(index < 0){
                throw DataXException.asDataXException(Hbase094xWriterErrorCode.ILLEGAL_VALUE, "您versionColumn配置项中index配置不正确,只能取-1或者非负数");
            }
        }
    }
}
