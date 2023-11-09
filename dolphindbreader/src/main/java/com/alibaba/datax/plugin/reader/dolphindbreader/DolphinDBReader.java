package com.alibaba.datax.plugin.reader.dolphindbreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import com.xxdb.data.*;
import com.xxdb.data.Vector;
import com.xxdb.io.ProgressListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.time.*;
import java.util.*;
import java.util.stream.IntStream;

public class DolphinDBReader extends Reader {

    private static final String DOLPHINDB_DATAX_READER_VERSION = "1.30.22.3";
    private static final String PARTITION = "partition";
    private static String TABLE_HANDLE = "";

    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration readerConfig = null;
        private DBConnection connection = null;
        private Entity dataSources = null;

        @Override
        public void init() {
            try {
                this.readerConfig = this.getPluginJobConf();
                Utils.validateParameter(this.readerConfig);

                if(readerConfig.getString(Key.DB_PATH) == null || readerConfig.getString(Key.DB_PATH).isEmpty())
                    TABLE_HANDLE = readerConfig.getString(Key.TABLE_NAME);
                else
                    TABLE_HANDLE = String.format("loadTable(\"%s\",`%s)", readerConfig.getString(Key.DB_PATH), readerConfig.getString(Key.TABLE_NAME));

                this.connection = Utils.connectDB(readerConfig);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            LOG.info("DolphinDBReader readerConfig: {}", this.readerConfig.toJSON());
        }

        @Override
        public void prepare() {
            super.prepare();
            String querySql = this.readerConfig.getString(Key.QUERY_SQL);

            if (Objects.nonNull(querySql) && !querySql.equals("")) {
                try {
                    this.dataSources = connection.run("ds=sqlDS(<" + querySql + ">); ds;");
                } catch (IOException e) {
                    // disable take 'querySql' to sqlDS split logic.
                    if (e.getMessage().contains("This SQL query can't split into multiple data sources")) {
                        LOG.info("Error happened when prepare dataSources, Information: This querySql can't split into multiple data sources to execute. Next will try to run querySql in one task.");
                        return;
                    } else {
                        LOG.error("Error happened when prepare dataSources, Information: " + e.getMessage() + ". Next will try to run querySql in one task.");
                        return;
                    }
                }
            } else {
                try {
                    this.dataSources = connection.run(String.format("ds=sqlDS(<select * from %s>); ds;", TABLE_HANDLE));
                } catch (IOException e) {
                    LOG.error("Error happened when prepare dataSources, Information:" + e.getMessage());
                }
            }


            if(!this.dataSources.isVector())
                throw new RuntimeException("The partition result is not a vector");
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            if (adviceNumber == 0){
                LOG.error("Invalid parameter, the number of channel is zero.");
                return null;
            }

            List<Configuration> configurationList = new ArrayList<>();
            if (Objects.nonNull(this.dataSources)) {
                Vector dataSourcesVec = (Vector) this.dataSources;
                int len = dataSourcesVec.rows();
                Map<Integer, List<String>> partitionConfigMap = new HashMap<>();

                for (int i = 0; i < len; i++) {
                    String dataSourceStr = dataSourcesVec.getString(i);
                    int index = i % adviceNumber;
                    partitionConfigMap.computeIfAbsent(index, k -> new ArrayList<>()).add(dataSourceStr);
                }

                if (adviceNumber <= len) {
                    for (int i = 0; i < adviceNumber; i++) {
                        // clone readerConfig's copy to set every new config.
                        Configuration tempReaderConfig = this.readerConfig.clone();
                        tempReaderConfig.set(PARTITION, partitionConfigMap.get(i));
                        configurationList.add(tempReaderConfig);
                    }
                } else {
                    for (int i = 0; i < len; i++) {
                        // clone readerConfig's copy to set every new config.
                        Configuration tempReaderConfig = this.readerConfig.clone();
                        tempReaderConfig.set(PARTITION, partitionConfigMap.get(i));
                        configurationList.add(tempReaderConfig);
                    }
                }
            } else {
                    configurationList.add(readerConfig);
            }

            return configurationList;
        }

        @Override
        public void destroy() {
            if (connection != null)
                connection.close();
        }
    }

    public static class Task extends Reader.Task {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration readerConfig = null;
        private DBConnection dbConnection = null;
        private List<String> executorySqls = null;
        private List<String> cols = null;
        private String dbPath = "";
        private String tableName = "";
        private String where = "";
        private String querySql = "";

        @Override
        public void init() {
            try {
                this.readerConfig = super.getPluginJobConf();
                dbConnection = Utils.connectDB(readerConfig);

                this.dbPath = this.readerConfig.getString(Key.DB_PATH);
                this.tableName = this.readerConfig.getString(Key.TABLE_NAME);
                this.where = this.readerConfig.getString(Key.WHERE);
                List<Object> tableField = this.readerConfig.getList(Key.COLUMNS);
                JSONArray fieldArr = JSONArray.parseArray(JSON.toJSONString(tableField));
                this.querySql = this.readerConfig.getString(Key.QUERY_SQL);

                // generate executorySqls.
                this.executorySqls = generateSQL(fieldArr);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }

        @Override
        public void startRead(RecordSender recordSender) {
            LOG.info("Start to read from DolphinDB.");
            BasicTable bt = null;
            if (Objects.nonNull(this.executorySqls)) {
                try {
                    for (String executorySql : executorySqls) {
                        EntityBlockReader v = (EntityBlockReader) dbConnection.run(executorySql, (ProgressListener) null, 4, 4, 10000);
                        bt = (BasicTable) v.read();
                        if (Objects.nonNull(querySql) && !querySql.isEmpty() && Objects.isNull(this.cols))
                            initCols(bt);
                        sendData(bt, recordSender, executorySql);
                        while(v.hasNext()){
                            bt = (BasicTable) v.read();
                            sendData(bt, recordSender, executorySql);
                        }

                    }
                } catch (IOException e) {
                    LOG.error(e.getMessage(), e);
                }
            } else if (Objects.nonNull(querySql) && !querySql.isEmpty()) {
                try {
                    EntityBlockReader v = (EntityBlockReader) dbConnection.run(querySql, (ProgressListener) null, 4, 4, 10000);
                    bt = (BasicTable) v.read();
                    initCols(bt);
                    sendData(bt, recordSender, querySql);
                    while(v.hasNext()){
                        bt = (BasicTable) v.read();
                        sendData(bt, recordSender, querySql);
                    }
                } catch (IOException e) {
                    LOG.error("Error happened when run querySql, please check your querySql: " + e.getMessage());
                }
            }
        }

        public void sendData(BasicTable bt, RecordSender recordSender, String executorySql){
            try {
                Map<String, Vector> columnCache = new HashMap<>();
                for (String col : this.cols) {
                    columnCache.put(col, bt.getColumn(col));
                }

                for (int i = 0; i < bt.rows(); i++){
                    Column column = null;
                    Record record = recordSender.createRecord();
                    for (String col : this.cols) {
                        Vector vec = columnCache.get(col);
                        if (Objects.isNull(vec))
                            continue;

                        Entity.DATA_TYPE dataType = vec.getDataType();
                        switch (dataType) {
                            case DT_FLOAT:
                                BasicFloatVector floatVec = (BasicFloatVector)  vec;
                                column = floatVec.isNull(i) ? new DoubleColumn((Float)null) : new DoubleColumn(floatVec.getFloat(i));
                                break;
                            case DT_DOUBLE:
                                BasicDoubleVector doubleVec = (BasicDoubleVector) vec;
                                column = doubleVec.isNull(i) ? new DoubleColumn((Double)null) : new DoubleColumn(doubleVec.getDouble(i));
                                break;
                            case DT_BOOL:
                                BasicBooleanVector booleanVec = (BasicBooleanVector) vec;
                                column = booleanVec.isNull(i) ? new BoolColumn((Boolean) null) : new BoolColumn(booleanVec.getBoolean(i));
                                break;
                            case DT_DATE:
                                BasicDateVector dateVec = (BasicDateVector) vec;
                                if( dateVec.isNull(i)  ){
                                    column = new DateColumn((Date) null);
                                }else{
                                    column = new DateColumn(Date.from(dateVec.getDate(i).atTime(LocalTime.MIDNIGHT).atZone(ZoneId.systemDefault()).toInstant()));
                                }
                                break;
                            case DT_DATETIME:
                                BasicDateTimeVector dateTimeVec = (BasicDateTimeVector) vec;
                                if( dateTimeVec.isNull(i) ){
                                    column = new DateColumn((Date) null);
                                }else{
                                    column = new DateColumn(Date.from(dateTimeVec.getDateTime(i).atZone( ZoneId.systemDefault()).toInstant()));
                                }
                                break;
                            case DT_TIME:
                                BasicTimeVector timeVec = (BasicTimeVector) vec;
                                column = timeVec.isNull(i) ? new StringColumn((String) null) : new StringColumn(timeVec.getString(i));
                                break;
                            case DT_TIMESTAMP:
                                BasicTimestampVector timeStampVec = (BasicTimestampVector)  vec;
                                if( timeStampVec.isNull(i) ){
                                    column = new DateColumn((Date) null);
                                }else{
                                    column = new DateColumn(Date.from(timeStampVec.getTimestamp(i).atZone( ZoneId.systemDefault()).toInstant()));
                                }
                                break;
                            case DT_NANOTIME:
                                BasicNanoTimeVector nanoTimeVec = (BasicNanoTimeVector) vec;
                                column = nanoTimeVec.isNull(i) ? new StringColumn((String) null) : new StringColumn(nanoTimeVec.getString(i));
                                break;
                            case DT_NANOTIMESTAMP:
                                BasicNanoTimestampVector nanoTimestampVec = (BasicNanoTimestampVector)  vec;
                                if( nanoTimestampVec.isNull(i) ){
                                    column = new DateColumn((Date) null);
                                }else{
                                    column = new DateColumn(Date.from(nanoTimestampVec.getNanoTimestamp(i).atZone( ZoneId.systemDefault()).toInstant()));
                                }
                                break;
                            case DT_BYTE:
                                BasicByteVector byteVec = (BasicByteVector) vec;
                                column = byteVec.isNull(i) ? new LongColumn((Long) null) : new LongColumn((long)byteVec.getByte(i));
                                break;
                            case DT_LONG:
                                BasicLongVector longVec = (BasicLongVector)  vec;
                                column = longVec.isNull(i) ? new LongColumn((Long) null) : new LongColumn(longVec.getLong(i));
                                break;
                            case DT_SHORT:
                                BasicShortVector shortVec = (BasicShortVector)  vec;
                                column = shortVec.isNull(i) ? new LongColumn((Long) null) : new LongColumn((long)shortVec.getShort(i));
                                break;
                            case DT_INT:
                                BasicIntVector intVec = (BasicIntVector) vec;
                                column = intVec.isNull(i) ? new LongColumn((Long) null) : new LongColumn(intVec.getInt(i));
                                break;
                            case DT_UUID:
                                BasicUuidVector uuidVector = (BasicUuidVector) vec;
                                column = uuidVector.isNull(i) ? new StringColumn((String) null) : new StringColumn(uuidVector.get(i).getString());
                                break;
                            case DT_BLOB:
                            case DT_STRING:
                                BasicStringVector stringVec = (BasicStringVector) vec;
                                column = stringVec.isNull(i) ? new StringColumn((String) null) : new StringColumn(stringVec.getString(i));
                                break;
                            case DT_SYMBOL:
                                Vector symbolVec = vec;
                                column = symbolVec.isNull(i) ? new StringColumn((String) null) : new StringColumn(symbolVec.getString(i));
                                break;
                            case DT_COMPLEX:
                                BasicComplexVector complexVec = (BasicComplexVector) vec;
                                column = complexVec.isNull(i) ? new StringColumn((String) null) : new StringColumn(complexVec.getString(i));
                                break;
                            case DT_DATEHOUR:
                                BasicDateHourVector dateHourVec = (BasicDateHourVector) vec;
                                if( dateHourVec.isNull(i) ){
                                    column = new DateColumn((Date) null);
                                }else{
                                    column = new DateColumn(Date.from(dateHourVec.getDateHour(i).atZone( ZoneId.systemDefault()).toInstant()));
                                }
                                break;
                            case DT_DURATION:
                                BasicDurationVector durationVec = (BasicDurationVector) vec;
                                column = durationVec.isNull(i) ? new LongColumn((Long) null) : new LongColumn(durationVec.getString(i));
                                break;
                            case DT_INT128:
                                BasicInt128Vector int128Vec = (BasicInt128Vector) vec;
                                column = int128Vec.isNull(i) ? new StringColumn((String) null) : new StringColumn(int128Vec.getString(i));
                                break;
                            case DT_IPADDR:
                                BasicIPAddrVector ipaddrVec = (BasicIPAddrVector) vec;
                                column = ipaddrVec.isNull(i) ? new StringColumn((String) null) : new StringColumn(ipaddrVec.getString(i));
                                break;
                            case DT_MINUTE:
                                BasicMinuteVector minuteVec = (BasicMinuteVector) vec;
                                column = minuteVec.isNull(i) ? new StringColumn((String) null) : new StringColumn(minuteVec.getString(i));
                                break;
                            case DT_MONTH:
                                BasicMonthVector monthVec = (BasicMonthVector) vec;
                                column = monthVec.isNull(i) ? new StringColumn((String) null) : new StringColumn(monthVec.getString(i));
                                break;
                            case DT_POINT:
                                BasicPointVector pointVec = (BasicPointVector) vec;
                                column = pointVec.isNull(i) ? new StringColumn((String) null) : new StringColumn(pointVec.getString(i));
                                break;
                            case DT_SECOND:
                                BasicSecondVector secondVec = (BasicSecondVector) vec;
                                column = secondVec.isNull(i) ? new StringColumn((String) null) : new StringColumn(secondVec.getString(i));
                                break;
                            case DT_DECIMAL32:
                                BasicDecimal32Vector decimal32Vector = (BasicDecimal32Vector) vec;
                                column = decimal32Vector.isNull(i) ? new StringColumn(null) : new StringColumn(decimal32Vector.getString(i));
                                break;
                            case DT_DECIMAL64:
                                BasicDecimal64Vector decimal64Vector = (BasicDecimal64Vector) vec;
                                column = decimal64Vector.isNull(i) ? new StringColumn(null) : new StringColumn(decimal64Vector.getString(i));
                                break;
                            case DT_DECIMAL128:
                                BasicDecimal128Vector decimal128Vector = (BasicDecimal128Vector) vec;
                                column = decimal128Vector.isNull(i) ? new StringColumn(null) : new StringColumn(decimal128Vector.getString(i));
                                break;
                            case DT_VOID:
                                column = new StringColumn((String) null);
                                break;
                            default:
                                LOG.info("Unsupported DataType!!!");
                                break;
                        }
                        record.addColumn(column);
                    }
                    recordSender.sendToWriter(record);
                }
            } catch (Exception ex) {
                LOG.error(ex.getMessage(), ex);
            }

            LOG.info("Value Send Success. " + executorySql);
        }

        private void initCols(JSONArray fieldArr) {
            this.cols = new ArrayList<>();
            if (fieldArr.toString().equals("[]")) {
                try {
                    BasicDictionary schema = (BasicDictionary) dbConnection.run(TABLE_HANDLE + ".schema()");
                    BasicTable colDefs = (BasicTable) schema.get(new BasicString("colDefs"));
                    BasicStringVector colNames = (BasicStringVector) colDefs.getColumn("name");

                    IntStream.range(0, colDefs.rows()).forEach(i -> this.cols.add(colNames.getString(i)));
                } catch (Exception e){
                    LOG.error(e.getMessage(), e);
                }
            } else {
                for (int i = 0; i < fieldArr.size(); i++) {
                    JSONObject field = fieldArr.getJSONObject(i);
                    this.cols.add(field.getString("name"));
                }
            }
        }

        private void initCols(BasicTable bt) {
            this.cols = new ArrayList<>();
            for (int i = 0; i < bt.columns(); i ++)
                this.cols.add(bt.getColumnName(i));
        }

        private List<String> generateSQL(JSONArray fieldArr) {
            StringBuilder colsBuilder = new StringBuilder();
            if (Objects.nonNull(this.querySql) && !this.querySql.isEmpty()) {
                // if set 'querySql', disable 'Where' and  'Columns' param.
                this.where = null;
                fieldArr = null;

                try {
                    dbConnection.run("ds=sqlDS(<" + this.querySql + ">); ds;");
                } catch (IOException e) {
                    if (e.getMessage().contains("This SQL query can't split into multiple data sources")) {
                        // disable put 'querySql' to sqlDS split, only execute querySql
                        LOG.info("Error happened when prepare dataSources, Information: This querySql can't split into multiple data sources to execute. Next will try to run querySql in one task.");
                        return null;
                    } else {
                        LOG.error("Error happened when prepare dataSources, Information: " + e.getMessage() + ". Next will try to run querySql in one task.");
                        return null;
                    }
                }
            } else {
                initCols(fieldArr);
                // if not set 'querySql', build table col's sql part.
                for (int i = 0; i < this.cols.size(); i++){
                    if (i != this.cols.size() - 1)
                        colsBuilder.append(this.cols.get(i)).append(",");
                    else
                        colsBuilder.append(this.cols.get(i));
                }

                try {
                    dbConnection.run(String.format("ds=sqlDS(<select * from %s>); ds;", TABLE_HANDLE));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            // try to build ds executorySqls.
            List<Object> dsSqls = this.readerConfig.getList(PARTITION);
            List<String> executorySqlsList = new ArrayList<>();
            for (Object dsSqlObj : dsSqls) {
                String dsSql = (String) dsSqlObj;
                String transferedDsSql = dsSql.replace("\"", "\\\"");
                String script = String.format("temp=size(ds)-1; exec index from table(0..temp as index,ds.string() as str) where str = \"%s\"", transferedDsSql);
                try {
                    Entity datasourceIndex = dbConnection.run(script);
                    if (!datasourceIndex.isVector()) {
                        LOG.error("The datasource list is not a vector. Next will try to run querySql in current task.");
                        return null;
                    } else {
                        Vector datasourceIndexVec = (Vector) datasourceIndex;
                        for (int i = 0; i < datasourceIndexVec.rows() ;i++){
                            // 注：到这一步，就不需要考虑 querySql 了，意思是不需要要把 querySql 拼到 sql 中了
                            if (Objects.nonNull(this.querySql) && !this.querySql.isEmpty()) {
                                executorySqlsList.add(String.format("select * from ds[%s]", datasourceIndexVec.getString(i)));
                            } else {
                                if (this.where == null || this.where.isEmpty()) {
                                    if (fieldArr.toString().equals("[]"))
                                        executorySqlsList.add(String.format("select * from ds[%s]", datasourceIndexVec.getString(i)));
                                    else
                                        executorySqlsList.add(String.format("select %s from ds[%s]", colsBuilder, datasourceIndexVec.getString(i)));
                                } else {
                                    if (fieldArr.toString().equals("[]"))
                                        executorySqlsList.add(String.format("select * from ds[%s] where " + this.where, datasourceIndexVec.getString(i))) ;
                                    else
                                        executorySqlsList.add(String.format("select %s from ds[%s] where " + this.where, colsBuilder, datasourceIndexVec.getString(i))) ;
                                }
                            }
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            return executorySqlsList;
        }

        @Override
        public void destroy() {
            if (dbConnection != null){
                dbConnection.close();
            }
        }
    }
}
