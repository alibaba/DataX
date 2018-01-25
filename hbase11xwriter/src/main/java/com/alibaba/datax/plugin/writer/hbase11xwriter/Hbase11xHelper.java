package com.alibaba.datax.plugin.writer.hbase11xwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;


public class Hbase11xHelper {

    private static final Logger LOG = LoggerFactory.getLogger(Hbase11xHelper.class);

    public static org.apache.hadoop.conf.Configuration getHbaseConfiguration(String hbaseConfig) {
        if (StringUtils.isBlank(hbaseConfig)) {
            throw DataXException.asDataXException(Hbase11xWriterErrorCode.REQUIRED_VALUE, "读 Hbase 时需要配置hbaseConfig，其内容为 Hbase 连接信息，请联系 Hbase PE 获取该信息.");
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
            throw DataXException.asDataXException(Hbase11xWriterErrorCode.GET_HBASE_CONNECTION_ERROR, e);
        }
        return hConfiguration;
    }

    public static org.apache.hadoop.hbase.client.Connection getHbaseConnection(String hbaseConfig) {
        org.apache.hadoop.conf.Configuration hConfiguration = Hbase11xHelper.getHbaseConfiguration(hbaseConfig);

        org.apache.hadoop.hbase.client.Connection hConnection = null;
        try {
            hConnection = ConnectionFactory.createConnection(hConfiguration);

        } catch (Exception e) {
            Hbase11xHelper.closeConnection(hConnection);
            throw DataXException.asDataXException(Hbase11xWriterErrorCode.GET_HBASE_CONNECTION_ERROR, e);
        }
        return hConnection;
    }


    public static Table getTable(com.alibaba.datax.common.util.Configuration configuration){
        String hbaseConfig = configuration.getString(Key.HBASE_CONFIG);
        String userTable = configuration.getString(Key.TABLE);
        long writeBufferSize = configuration.getLong(Key.WRITE_BUFFER_SIZE, Constant.DEFAULT_WRITE_BUFFER_SIZE);
        org.apache.hadoop.hbase.client.Connection hConnection = Hbase11xHelper.getHbaseConnection(hbaseConfig);
        TableName hTableName = TableName.valueOf(userTable);
        org.apache.hadoop.hbase.client.Admin admin = null;
        org.apache.hadoop.hbase.client.Table hTable = null;
        try {
            admin = hConnection.getAdmin();
            Hbase11xHelper.checkHbaseTable(admin,hTableName);
            hTable = hConnection.getTable(hTableName);
            BufferedMutatorParams bufferedMutatorParams =  new BufferedMutatorParams(hTableName);
            bufferedMutatorParams.writeBufferSize(writeBufferSize);
        } catch (Exception e) {
            Hbase11xHelper.closeTable(hTable);
            Hbase11xHelper.closeAdmin(admin);
            Hbase11xHelper.closeConnection(hConnection);
            throw DataXException.asDataXException(Hbase11xWriterErrorCode.GET_HBASE_TABLE_ERROR, e);
        }
        return hTable;
    }

    public static BufferedMutator getBufferedMutator(com.alibaba.datax.common.util.Configuration configuration){
        String hbaseConfig = configuration.getString(Key.HBASE_CONFIG);
        String userTable = configuration.getString(Key.TABLE);
        long writeBufferSize = configuration.getLong(Key.WRITE_BUFFER_SIZE, Constant.DEFAULT_WRITE_BUFFER_SIZE);
        org.apache.hadoop.conf.Configuration hConfiguration = Hbase11xHelper.getHbaseConfiguration(hbaseConfig);
        org.apache.hadoop.hbase.client.Connection hConnection = Hbase11xHelper.getHbaseConnection(hbaseConfig);
        TableName hTableName = TableName.valueOf(userTable);
        org.apache.hadoop.hbase.client.Admin admin = null;
        BufferedMutator bufferedMutator = null;
        try {
            admin = hConnection.getAdmin();
            Hbase11xHelper.checkHbaseTable(admin,hTableName);
            //参考HTable getBufferedMutator()
            bufferedMutator = hConnection.getBufferedMutator(
                    new BufferedMutatorParams(hTableName)
                    .pool(HTable.getDefaultExecutor(hConfiguration))
                    .writeBufferSize(writeBufferSize));
        } catch (Exception e) {
            Hbase11xHelper.closeBufferedMutator(bufferedMutator);
            Hbase11xHelper.closeAdmin(admin);
            Hbase11xHelper.closeConnection(hConnection);
            throw DataXException.asDataXException(Hbase11xWriterErrorCode.GET_HBASE_BUFFEREDMUTATOR_ERROR, e);
        }
        return bufferedMutator;
    }

    public static void deleteTable(com.alibaba.datax.common.util.Configuration configuration) {
        String userTable = configuration.getString(Key.TABLE);
        LOG.info(String.format("由于您配置了deleteType delete,HBasWriter begins to delete table %s .", userTable));
        Scan scan = new Scan();
        org.apache.hadoop.hbase.client.Table hTable =Hbase11xHelper.getTable(configuration);
        ResultScanner scanner = null;
        try {
            scanner = hTable.getScanner(scan);
            for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                hTable.delete(new Delete(rr.getRow()));
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(Hbase11xWriterErrorCode.DELETE_HBASE_ERROR, e);
        }finally {
            if(scanner != null){
                scanner.close();
            }
            Hbase11xHelper.closeTable(hTable);
        }
    }

    public static void truncateTable(com.alibaba.datax.common.util.Configuration configuration) {
        String hbaseConfig = configuration.getString(Key.HBASE_CONFIG);
        String userTable = configuration.getString(Key.TABLE);
        LOG.info(String.format("由于您配置了 truncate 为true,HBasWriter begins to truncate table %s .", userTable));
        TableName hTableName = TableName.valueOf(userTable);
        org.apache.hadoop.hbase.client.Connection hConnection = Hbase11xHelper.getHbaseConnection(hbaseConfig);
        org.apache.hadoop.hbase.client.Admin admin = null;
        try{
            admin = hConnection.getAdmin();
            Hbase11xHelper.checkHbaseTable(admin,hTableName);
            admin.disableTable(hTableName);
            admin.truncateTable(hTableName,true);
        }catch (Exception e) {
            throw DataXException.asDataXException(Hbase11xWriterErrorCode.TRUNCATE_HBASE_ERROR, e);
        }finally {
            Hbase11xHelper.closeAdmin(admin);
            Hbase11xHelper.closeConnection(hConnection);
        }
    }

    public static void closeConnection(Connection hConnection){
        try {
            if(null != hConnection)
                hConnection.close();
        } catch (IOException e) {
            throw DataXException.asDataXException(Hbase11xWriterErrorCode.CLOSE_HBASE_CONNECTION_ERROR, e);
        }
    }

    public static void closeAdmin(Admin admin){
        try {
            if(null != admin)
                admin.close();
        } catch (IOException e) {
            throw DataXException.asDataXException(Hbase11xWriterErrorCode.CLOSE_HBASE_AMIN_ERROR, e);
        }
    }

    public static void closeBufferedMutator(BufferedMutator bufferedMutator){
        try {
            if(null != bufferedMutator)
                bufferedMutator.close();
        } catch (IOException e) {
            throw DataXException.asDataXException(Hbase11xWriterErrorCode.CLOSE_HBASE_BUFFEREDMUTATOR_ERROR, e);
        }
    }

    public static void closeTable(Table table){
        try {
            if(null != table)
                table.close();
        } catch (IOException e) {
            throw DataXException.asDataXException(Hbase11xWriterErrorCode.CLOSE_HBASE_TABLE_ERROR, e);
        }
    }


    private static  void checkHbaseTable(Admin admin,  TableName hTableName) throws IOException {
        if(!admin.tableExists(hTableName)){
            throw DataXException.asDataXException(Hbase11xWriterErrorCode.ILLEGAL_VALUE, "HBase源头表" + hTableName.toString()
                    + "不存在, 请检查您的配置 或者 联系 Hbase 管理员.");
        }
        if(!admin.isTableAvailable(hTableName)){
            throw DataXException.asDataXException(Hbase11xWriterErrorCode.ILLEGAL_VALUE, "HBase源头表" +hTableName.toString()
                    + " 不可用, 请检查您的配置 或者 联系 Hbase 管理员.");
        }
        if(admin.isTableDisabled(hTableName)){
            throw DataXException.asDataXException(Hbase11xWriterErrorCode.ILLEGAL_VALUE, "HBase源头表" +hTableName.toString()
                    + "is disabled, 请检查您的配置 或者 联系 Hbase 管理员.");
        }
    }


    public static void validateParameter(com.alibaba.datax.common.util.Configuration originalConfig) {
        originalConfig.getNecessaryValue(Key.HBASE_CONFIG, Hbase11xWriterErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.TABLE, Hbase11xWriterErrorCode.REQUIRED_VALUE);

        Hbase11xHelper.validateMode(originalConfig);

        String encoding = originalConfig.getString(Key.ENCODING, Constant.DEFAULT_ENCODING);
        if (!Charset.isSupported(encoding)) {
            throw DataXException.asDataXException(Hbase11xWriterErrorCode.ILLEGAL_VALUE, String.format("Hbasewriter 不支持您所配置的编码:[%s]", encoding));
        }
        originalConfig.set(Key.ENCODING, encoding);

        Boolean walFlag = originalConfig.getBool(Key.WAL_FLAG, false);
        originalConfig.set(Key.WAL_FLAG, walFlag);
        long writeBufferSize = originalConfig.getLong(Key.WRITE_BUFFER_SIZE,Constant.DEFAULT_WRITE_BUFFER_SIZE);
        originalConfig.set(Key.WRITE_BUFFER_SIZE, writeBufferSize);
    }




    private  static  void validateMode(com.alibaba.datax.common.util.Configuration originalConfig){
        String mode = originalConfig.getNecessaryValue(Key.MODE,Hbase11xWriterErrorCode.REQUIRED_VALUE);
        ModeType modeType = ModeType.getByTypeName(mode);
        switch (modeType) {
            case Normal: {
                validateRowkeyColumn(originalConfig);
                validateColumn(originalConfig);
                validateVersionColumn(originalConfig);
                break;
            }
            default:
                throw DataXException.asDataXException(Hbase11xWriterErrorCode.ILLEGAL_VALUE,
                        String.format("Hbase11xWriter不支持该 mode 类型:%s", mode));
        }
    }

    private static void validateColumn(com.alibaba.datax.common.util.Configuration originalConfig){
        List<Configuration> columns = originalConfig.getListConfiguration(Key.COLUMN);
        if (columns == null || columns.isEmpty()) {
            throw DataXException.asDataXException(Hbase11xWriterErrorCode.REQUIRED_VALUE, "column为必填项，其形式为：column:[{\"index\": 0,\"name\": \"cf0:column0\",\"type\": \"string\"},{\"index\": 1,\"name\": \"cf1:column1\",\"type\": \"long\"}]");
        }
        for (Configuration aColumn : columns) {
            Integer index = aColumn.getInt(Key.INDEX);
            String type = aColumn.getNecessaryValue(Key.TYPE,Hbase11xWriterErrorCode.REQUIRED_VALUE);
            String name = aColumn.getNecessaryValue(Key.NAME,Hbase11xWriterErrorCode.REQUIRED_VALUE);
            ColumnType.getByTypeName(type);
            if(name.split(":").length != 2){
                throw DataXException.asDataXException(Hbase11xWriterErrorCode.ILLEGAL_VALUE, String.format("您column配置项中name配置的列格式[%s]不正确，name应该配置为 列族:列名  的形式, 如 {\"index\": 1,\"name\": \"cf1:q1\",\"type\": \"long\"}", name));
            }
            if(index == null || index < 0){
                throw DataXException.asDataXException(Hbase11xWriterErrorCode.ILLEGAL_VALUE, "您的column配置项不正确,配置项中中index为必填项,且为非负数，请检查并修改.");
            }
        }
    }

    private static void validateRowkeyColumn(com.alibaba.datax.common.util.Configuration originalConfig){
        List<Configuration> rowkeyColumn = originalConfig.getListConfiguration(Key.ROWKEY_COLUMN);
        if (rowkeyColumn == null || rowkeyColumn.isEmpty()) {
            throw DataXException.asDataXException(Hbase11xWriterErrorCode.REQUIRED_VALUE, "rowkeyColumn为必填项，其形式为：rowkeyColumn:[{\"index\": 0,\"type\": \"string\"},{\"index\": -1,\"type\": \"string\",\"value\": \"_\"}]");
        }
        int rowkeyColumnSize = rowkeyColumn.size();
        //包含{"index":0,"type":"string"} 或者 {"index":-1,"type":"string","value":"_"}
        for (Configuration aRowkeyColumn : rowkeyColumn) {
            Integer index = aRowkeyColumn.getInt(Key.INDEX);
            String type = aRowkeyColumn.getNecessaryValue(Key.TYPE,Hbase11xWriterErrorCode.REQUIRED_VALUE);
            ColumnType.getByTypeName(type);
            if(index == null ){
                throw DataXException.asDataXException(Hbase11xWriterErrorCode.REQUIRED_VALUE, "rowkeyColumn配置项中index为必填项");
            }
            //不能只有-1列,即rowkey连接串
            if(rowkeyColumnSize ==1 && index == -1){
                throw DataXException.asDataXException(Hbase11xWriterErrorCode.ILLEGAL_VALUE, "rowkeyColumn配置项不能全为常量列,至少指定一个rowkey列");
            }
            if(index == -1){
                aRowkeyColumn.getNecessaryValue(Key.VALUE,Hbase11xWriterErrorCode.REQUIRED_VALUE);
            }
        }
    }

    private static void validateVersionColumn(com.alibaba.datax.common.util.Configuration originalConfig){
        Configuration versionColumn = originalConfig.getConfiguration(Key.VERSION_COLUMN);
        //为null,表示用当前时间;指定列,需要index
        if(versionColumn != null){
            Integer index = versionColumn.getInt(Key.INDEX);
            if(index == null ){
                throw DataXException.asDataXException(Hbase11xWriterErrorCode.REQUIRED_VALUE, "versionColumn配置项中index为必填项");
            }
            if(index == -1){
                //指定时间,需要index=-1,value
                versionColumn.getNecessaryValue(Key.VALUE,Hbase11xWriterErrorCode.REQUIRED_VALUE);
            }else if(index < 0){
                throw DataXException.asDataXException(Hbase11xWriterErrorCode.ILLEGAL_VALUE, "您versionColumn配置项中index配置不正确,只能取-1或者非负数");
            }
        }
    }
}
