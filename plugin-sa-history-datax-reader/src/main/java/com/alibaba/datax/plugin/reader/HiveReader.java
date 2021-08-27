package com.alibaba.datax.plugin.reader;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.KeyConstant;
import com.alibaba.datax.plugin.ReaderErrorCode;
import com.alibaba.datax.plugin.util.HiveUtil;
import com.alibaba.datax.plugin.util.TypeUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class HiveReader extends Reader {

    @Slf4j
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Job extends Reader.Job{

        private Configuration originalConfig = null;

        private String  rowNumCountSql;
        private String  userName;
        private String  password;
        private String  hiveUrl;
        private String  sql;
        /**
         * 当使用时间条件过滤时，如果数据量大到不能拆分时，使用分页查询的sql
         */
        private String  sqlRowNum;
        private String  sqlCount;
        private String  rowNumSql;
        private String  where;
        private String  tableName;
        private List<String>  columnList;
        private boolean useRowNumber;

        @Override
        public List<Configuration> split(int i) {

            List<Configuration> splittedConfigs = new ArrayList<Configuration>();

            Boolean useRowNumber = originalConfig.getBool(KeyConstant.USE_ROW_NUMBER, false);
            if(useRowNumber){
                long pageSize = originalConfig.getLong(KeyConstant.PAGE_SIZE,10000);
                long receivePageSize = originalConfig.getLong(KeyConstant.RECEIVE_PAGE_SIZE,5);
                List<List<Long>> rowNumberPartition = supportRowNumberPartition(pageSize,receivePageSize);
                rowNumberPartition.forEach(item->{
                    Configuration sliceConfig = originalConfig.clone();
                    sliceConfig.set(KeyConstant.START_PAGE_NO,item.get(0));
                    sliceConfig.set(KeyConstant.END_PAGE_NO,item.get(1));
                    sliceConfig.set(KeyConstant.PAGE_SIZE,pageSize);
                    splittedConfigs.add(sliceConfig);
                });

            }else{
                String startTime = originalConfig.getString(KeyConstant.START_TIME);
                String endTime = originalConfig.getString(KeyConstant.END_TIME);
                Integer taskNum = originalConfig.getInt(KeyConstant.TASK_NUM,i);
                String datePattern = originalConfig.getString(KeyConstant.DATE_PATTERN);

                if(StrUtil.isBlank(startTime) || StrUtil.isBlank(endTime) || StrUtil.isBlank(datePattern)){
                    throw new DataXException(CommonErrorCode.CONFIG_ERROR,"useRowNumber为空或false时，startTime/endTime/datePattern不能为空");
                }

                SimpleDateFormat sdf = new SimpleDateFormat(datePattern);
                try {
                    Date startDate = sdf.parse(startTime);
                    Date endDate = sdf.parse(endTime);
                    if (startDate.after(endDate)) {
                        throw new DataXException(CommonErrorCode.CONFIG_ERROR,"startTime not should endTime after.");
                    }
                    long startNano = startDate.getTime();
                    long endNano = endDate.getTime();
                    long oneDayNano = 1000*60*60*24*1;
                    if(endNano-startNano <= oneDayNano){
                        taskNum = 1;
                    }
                    List<List<Long>> timePartition = partition(startNano,endNano,taskNum,null);
                    timePartition.forEach(time->{
                        Configuration sliceConfig = originalConfig.clone();
                        Date s = new Date(time.get(0));
                        Date e = new Date(time.get(1));
                        sliceConfig.set(KeyConstant.TASK_START_TIME,sdf.format(s));
                        sliceConfig.set(KeyConstant.TASK_END_TIME,sdf.format(e));
                        splittedConfigs.add(sliceConfig);
                        s = null;
                        e = null;
                    });

                } catch (ParseException e) {
                    e.printStackTrace();
                }

            }

            return splittedConfigs;
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            String where = originalConfig.getString(KeyConstant.WHERE,"");
            this.tableName = originalConfig.getString(KeyConstant.SA.concat(KeyConstant.POINT).concat(KeyConstant.SA_TABLE));
            this.userName = originalConfig.getString(KeyConstant.USER_NAME,"");
            this.password = originalConfig.getString(KeyConstant.PASSWORD,"");
            this.hiveUrl = originalConfig.getString(KeyConstant.SA.concat(KeyConstant.POINT).concat(KeyConstant.SA_HIVE_URL));
            if(StrUtil.isBlank(this.tableName) || StrUtil.isBlank(this.hiveUrl)){
                throw new DataXException(CommonErrorCode.CONFIG_ERROR,"hiveUrl和table不能为空");
            }
            this.rowNumCountSql = "select count(*) from ".concat(tableName).concat(StrUtil.isBlank(where)?"":"  where ".concat(where));
            HiveUtil.setUrl(this.hiveUrl);
            HiveUtil.setUser(this.userName);
            HiveUtil.setPassword(this.password);

            String timeFieldName = originalConfig.getString(KeyConstant.TIME_FIELD_NAME,"");
            this.columnList = originalConfig.getList(KeyConstant.COLUMN, String.class);
            if(Objects.isNull(columnList) || columnList.isEmpty() ){
                throw new DataXException(CommonErrorCode.CONFIG_ERROR,"column不能为空！");
            }
            String columnStr = CollectionUtil.join(this.columnList, ",");

            this.useRowNumber = originalConfig.getBool(KeyConstant.USE_ROW_NUMBER, true);
            if(!useRowNumber){
                String startTime = originalConfig.getString(KeyConstant.START_TIME,"");
                String endTime = originalConfig.getString(KeyConstant.END_TIME,"");
                if(StrUtil.isBlank(timeFieldName) || StrUtil.isBlank(startTime) || StrUtil.isBlank(endTime) ){
                    throw new DataXException(CommonErrorCode.CONFIG_ERROR,"配置有误，请检查！");
                }
            }

            this.sql = "select ".concat(columnStr).concat(" from ").concat(this.tableName).concat(" where ")
                    .concat(timeFieldName).concat(" >= '{}' and ").concat(timeFieldName).concat(" < '{}'")
                    .concat(StrUtil.isBlank(where)?"":"  and ".concat(where));

            this.sqlRowNum = "select t.* ".concat(" from ( select row_number() over () as internalrnum, ")
                    .concat(columnStr).concat(" from ").concat(this.tableName).concat(" where ")
                    .concat(timeFieldName).concat(" >= '{}' and ").concat(timeFieldName).concat(" < '{}'")
                    .concat(StrUtil.isBlank(where)?"":"  and ".concat(where))
                    .concat(" limit {} ) t ").concat(" where internalrnum between {} and {} ");

            this.sqlCount = "select count(*) ".concat(" from ").concat(this.tableName).concat(" where ")
                    .concat(timeFieldName).concat(" >= '{}' and ").concat(timeFieldName).concat(" < '{}'")
                    .concat(StrUtil.isBlank(where)?"":"  and ".concat(where));

            this.rowNumSql = "select t.* ".concat(" from ( select row_number() over () as internalrnum, ")
                    .concat(columnStr).concat(" from ").concat(this.tableName)
                    .concat(StrUtil.isBlank(where)?"":"  where ".concat(where))
                    .concat(" limit {} ) t ").concat(" where internalrnum between {} and {} ");

            originalConfig.set(KeyConstant.SQL_TEMPLATE,this.sql);
            originalConfig.set(KeyConstant.SQL_COUNT_TEMPLATE,this.sqlCount);
            originalConfig.set(KeyConstant.ROW_NUM_SQL_TEMPLATE,this.rowNumSql);
            originalConfig.set(KeyConstant.SQL_ROW_NUM_TEMPLATE,this.sqlRowNum);

        }

        @Override
        public void destroy() {}

        /**
         * 使用hive row_number函数方式分区
         * @param pageSize
         * @param receivePageSize
         * @return
         */
        public List<List<Long>> supportRowNumberPartition(long pageSize,long receivePageSize){
            List<List<Long>> list = new ArrayList<>();
            JdbcTemplate hiveJdbc = HiveUtil.defaultJdbcTemplate();
            Double size = hiveJdbc.queryForObject(this.rowNumCountSql, Double.class);
            long pageNo = 0;
            long rounds = ((Double)Math.ceil(size / pageSize)).longValue();
            if(rounds <= 1){
                List<Long> l = new ArrayList<>();
                l.add(++pageNo);
                l.add(pageNo);
                list.add(l);
                return list;
            }
            while(pageNo < rounds){
                List<Long> l = new ArrayList<>();
                l.add(++pageNo);
                pageNo += receivePageSize-1;
                l.add(pageNo>rounds?rounds:pageNo);
                list.add(l);
            }
            log.info("当前总数据量：{}，pageSize：{}，每个任务负责的页数：{}，分配的任务数量：{}",size,pageSize,receivePageSize,list.size());
            return list;

        }

        /**
         * 将时间段分片
         * @param startNano 开始时间毫秒值
         * @param endNano 结束时间毫秒值
         * @param taskNum 切分数
         * @param stepSize 步长，若传递此参数则taskNum不生效
         * @return
         */
        public static List<List<Long>> partition(long startNano, long endNano, Integer taskNum,Long stepSize) {

            List<List<Long>> list = new ArrayList<>();

            if(stepSize != null){
                while(startNano < endNano){
                    List<Long> l = new ArrayList<>();
                    l.add(startNano);
                    startNano = startNano + stepSize;
                    if(startNano > endNano){
                        startNano = endNano;
                    }
                    l.add(startNano);
                    list.add(l);
                }
                return list;
            }

            if(taskNum <= 1){
                List<Long> l = new ArrayList<>();
                l.add(startNano);
                l.add(endNano);
                list.add(l);
                return list;
            }else{
                long num = (endNano-startNano)/taskNum;
                while(startNano < endNano){
                    List<Long> l = new ArrayList<>();
                    l.add(startNano);
                    startNano = startNano + num;
                    if(startNano > endNano){
                        startNano = endNano;
                    }
                    l.add(startNano);
                    list.add(l);
                }

            }
            return list;

        }

    }


    @Slf4j
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Task extends Reader.Task {

        private Configuration readerConfig;

        private JdbcTemplate hiveJdbc;

        private String  startTime;

        private String  endTime;

        private String  sql;
        /**
         * 当使用时间条件过滤时，如果数据量大到不能拆分时，使用分页查询的sql
         */
        private String  sqlRowNum;

        private String  sqlCount;

        private Long  maxQueryNum;

        private String  rowNumSql;

        private String  timePattern;

        private Long timeInterval;
        //      时间分片方式是否统计拆分，默认值true,表示优先使用先统计数量再拉取值，当数据量大时可防止OOM,若为false,则直接拉取数据，不在进行统计，可能导致OOM
        private Boolean timeFieldCount;

        //       当使用时间条件过滤时，如果数据量大到不能拆分时，使用分页
        private Long pageSize;

        private SimpleDateFormat sdf;

        private List<String>  columnList;
        /**
         * 列的简称
         */
        private List<String>  columnNameList;

        private Map<String,Integer>  columnIndexMap;

        private boolean useRowNumber;

        @Override
        public void startRead(RecordSender recordSender) {
            long sum = 0;
            if(useRowNumber){
                Long startPageNo = readerConfig.getLong(KeyConstant.START_PAGE_NO);
                Long endPageNo = readerConfig.getLong(KeyConstant.END_PAGE_NO);
                Long pageSize = readerConfig.getLong(KeyConstant.PAGE_SIZE);

                if(Objects.isNull(startPageNo) || Objects.isNull(endPageNo) || Objects.isNull(pageSize)
                        || (startPageNo > endPageNo) || pageSize<=0){
                    return;
                }
                log.info("startPageNo:{},endPageNo:{},pageSize:{},start",startPageNo,endPageNo,pageSize);
                sum = supportRowNumber(startPageNo,endPageNo,pageSize,recordSender);
                log.info("startPageNo:{},endPageNo:{},pageSize:{},end------,本次查询总数：{}",startPageNo,endPageNo,pageSize,sum);
            }else{
                if(Objects.isNull(startTime) || Objects.isNull(endTime)){
                    return;
                }
                log.info("startTime:{},endTime:{},start",startTime,endTime);
                sum = notSupportRowNumber(startTime, endTime, recordSender);
                log.info("startTime:{},endTime:{},end------,本次查询总数：{}",startTime,endTime,sum);
            }
        }

        @Override
        public void init() {
            //获取到的是上面task的split方法返回的某一个
            this.readerConfig = super.getPluginJobConf();
            this.startTime = readerConfig.getString(KeyConstant.TASK_START_TIME,null);
            this.endTime = readerConfig.getString(KeyConstant.TASK_END_TIME,null);
            this.timePattern = readerConfig.getString(KeyConstant.DATE_PATTERN,null);
            this.timeFieldCount = readerConfig.getBool(KeyConstant.TIME_FIELD_COUNT,true);
            this.sdf = new SimpleDateFormat(this.timePattern);
            this.timeInterval = readerConfig.getLong(KeyConstant.TIME_INTERVAL,1000*60*60*24*1L);
            if(Objects.isNull(this.timeInterval) || this.timeInterval<=0){
                this.timeInterval = 1000*60*60*24*1L;
            }

            this.columnList = readerConfig.getList(KeyConstant.COLUMN, String.class);
            columnIndexMap = new HashMap<>(this.columnList.size());
            List<String>  columnNames = new ArrayList<>();
            for (int i = 0; i < this.columnList.size(); i++) {
                String col = this.columnList.get(i);
                columnIndexMap.put(col,i);
                col = col.contains(".")?col.substring(col.lastIndexOf(".")+1):col;
                col = col.contains(" ")?col.substring(col.lastIndexOf(" ")+1):col;
                columnNames.add(col);
            }
            this.columnNameList = columnNames;

            this.sql = readerConfig.getString(KeyConstant.SQL_TEMPLATE);
            this.sqlCount = readerConfig.getString(KeyConstant.SQL_COUNT_TEMPLATE);
            this.maxQueryNum = readerConfig.getLong(KeyConstant.MAX_QUERY_NUM,50000L);
            this.rowNumSql = readerConfig.getString(KeyConstant.ROW_NUM_SQL_TEMPLATE);;
            this.hiveJdbc = HiveUtil.defaultJdbcTemplate();
            this.useRowNumber = readerConfig.getBool(KeyConstant.USE_ROW_NUMBER, true);
            this.sqlRowNum = readerConfig.getString(KeyConstant.SQL_ROW_NUM_TEMPLATE);
            this.pageSize = readerConfig.getLong(KeyConstant.PAGE_SIZE,200000);
        }


        @Override
        public void destroy() {
            this.hiveJdbc = null;
            this.readerConfig = null;
        }


        private Record buildRecord(RecordSender recordSender, Map<String, Object> item){
            if(Objects.isNull(item) || item.isEmpty()){
                return null;
            }
            Record record = recordSender.createRecord();
            Map<String,String> keyMap = new HashMap<>();
            for (String key : item.keySet()) {
                String k = keyMap.get(keyMap);
                Object value = item.get(key);
                if(Objects.isNull(k)){
                    k = key.contains(".")?key.substring(key.lastIndexOf(".")+1):key;
                    keyMap.put(key,k);
                }
                if("internalrnum".equals(k)){
                    continue;
                }
                if (this.columnNameList.contains(k)) {
                    Integer index = columnIndexMap.get(k);
                    if(Objects.isNull(value)){
                        record.setColumn(index,new StringColumn((String) value));
                    }else if(value instanceof String){
                        record.setColumn(index,new StringColumn((String) value));
                    }else if(TypeUtil.isPrimitive(value,Boolean.class)){
                        record.setColumn(index,new BoolColumn(Boolean.parseBoolean(value.toString()) ));
                    }else if(TypeUtil.isPrimitive(value,Byte.class) || TypeUtil.isPrimitive(value,Short.class) || TypeUtil.isPrimitive(value,Integer.class) || TypeUtil.isPrimitive(value,Long.class)){
                        record.setColumn(index,new LongColumn(Long.parseLong(value.toString())));
                    }else if(TypeUtil.isPrimitive(value,Float.class) || TypeUtil.isPrimitive(value,Double.class)){
                        record.setColumn(index,new DoubleColumn(value.toString()));
                    }else if(value instanceof Date){
                        record.setColumn(index,new DateColumn( (Date)value) );
                    }else if(value instanceof LocalDate){
                        record.setColumn(index,new DateColumn( Date.from(((LocalDate)value).atStartOfDay().atZone(ZoneId.systemDefault()).toInstant()) ));
                    }else if(value instanceof LocalDateTime){
                        record.setColumn(index,new DateColumn( Date.from(((LocalDateTime)value).atZone(ZoneId.systemDefault()).toInstant()) ));
                    }else if(value instanceof java.sql.Date){
                        record.setColumn(index,new DateColumn( new Date(((java.sql.Date)value).getTime()) ));
                    }else if(value instanceof java.sql.Timestamp){
                        record.setColumn(index,new DateColumn( (Date)value) );
                    }
                    else{
                        DataXException dataXException = DataXException
                                .asDataXException(
                                        ReaderErrorCode.UNSUPPORTED_TYPE, String.format("不支持的数据类型type:%s,value:%s", value.getClass().getName(), value));
                        TaskPluginCollector taskPluginCollector = super.getTaskPluginCollector();
                        taskPluginCollector.collectDirtyRecord(record,dataXException);
                    }
                }
            }

            return record;
        }


        /**
         * 使用hive row_number函数查询分段
         * @param startPageNo
         * @param endPageNo
         * @param pageSize
         * @param recordSender
         * @return
         */
        private long supportRowNumber(Long startPageNo,Long endPageNo,Long pageSize,RecordSender recordSender){
            AtomicLong sum = new AtomicLong(0);
            for (; startPageNo <= endPageNo; startPageNo++) {
                sum.addAndGet(doSupportRowNumber(startPageNo,pageSize,recordSender));
            }
            return sum.get();
        }

        /**
         * 使用hive row_number函数查询分段
         * @param pageNo
         * @param pageSize
         * @param recordSender
         * @return
         */
        private long doSupportRowNumber(long pageNo,long pageSize,RecordSender recordSender){
            String sqlTmp = StrUtil.format(this.rowNumSql, pageNo*pageSize,pageSize*(pageNo-1)+1,pageNo*pageSize);
            log.info("sql:{}",sqlTmp);
            List<Map<String, Object>> data = hiveJdbc.queryForList(sqlTmp);
            if(Objects.isNull(data) || data.isEmpty()){
                return 0;
            }
            data.forEach(item->{
                Record record = this.buildRecord(recordSender, item);
                recordSender.sendToWriter(record);
            });
            return data.size();
        }


        /**
         * 使用时间查询分段
         * @param startTime
         * @param endTime
         * @param recordSender
         * @return
         */
        private long notSupportRowNumber(String startTime,String endTime,RecordSender recordSender){
            AtomicLong sum = new AtomicLong(0);
            try {
                Date start = sdf.parse(startTime);
                Date end = sdf.parse(endTime);
                long startNano = start.getTime();
                long endNano = end.getTime();

                //间隔大于一天，每次只查一天的数据
                String startStr = null;
                String endStr = null;
                long oneDayNano = this.timeInterval;
                if(endNano-startNano > oneDayNano){
                    while (endNano-startNano > oneDayNano){
                        startStr = sdf.format(new Date(startNano));
                        long endTmp = (startNano+oneDayNano)>endNano?endNano:(startNano+oneDayNano);
                        endStr = sdf.format(new Date(endTmp));
                        if(this.timeFieldCount){
                            sum.addAndGet(doNotSupportRowNumber(startStr,endStr,recordSender));
                        }else{
                            sum.addAndGet(doNotSupportRowNumberAndNotCount(startStr,endStr,recordSender));
                        }
                        startNano = endTmp;
                    }
                    //最后一次可能没到endNano，并且不满足大于oneDayNano
                    if( endNano-startNano > 0){
                        startStr = sdf.format(new Date(startNano));
                        if(this.timeFieldCount){
                            sum.addAndGet(doNotSupportRowNumber(startStr,endTime,recordSender));
                        }else{
                            sum.addAndGet(doNotSupportRowNumberAndNotCount(startStr,endTime,recordSender));
                        }

                    }
                }else{
                    if(this.timeFieldCount){
                        sum.addAndGet(doNotSupportRowNumber(startTime,endTime,recordSender));
                    }else{
                        sum.addAndGet(doNotSupportRowNumberAndNotCount(startTime,endTime,recordSender));
                    }
                }

            } catch (ParseException e) {
                log.error("error:{}",e);
            }

            return sum.get();

        }

        private long doNotSupportRowNumberAndNotCount(String startTime,String endTime,RecordSender recordSender){
            if(startTime.equals(endTime)){
                return 0;
            }
            String sqlTmp = StrUtil.format(this.sql, startTime, endTime);
            log.info("sql:{}",sqlTmp);
            List<Map<String, Object>> data = hiveJdbc.queryForList(sqlTmp);
            if(Objects.isNull(data) || data.isEmpty()){
                log.info("startTime:{},endTime:{}，size:0",startTime, endTime);
                return 0;
            }
            log.info("startTime:{},endTime:{}，size:{}",startTime, endTime,data.size());
            data.forEach(item->{
                Record record = this.buildRecord(recordSender, item);
                recordSender.sendToWriter(record);
            });
            return data.size();
        }


        /**
         * 使用时间查询分段
         * @param startTime
         * @param endTime
         * @param recordSender
         * @return
         */
        private long doNotSupportRowNumber(String startTime,String endTime,RecordSender recordSender){
            if(startTime.equals(endTime)){
                return 0;
            }
            String sqlCountTmp = StrUtil.format(this.sqlCount, startTime, endTime);
            Long count = hiveJdbc.queryForObject(sqlCountTmp, Long.class);
            long maxQueryNum = this.maxQueryNum;
            if(new Long(0).equals(count)){
                log.info("startTime:{},endTime:{},count:{}",startTime,endTime,0);
                return 0;
            }

            List<Map<String, Object>> data;
            if(count > maxQueryNum){
                log.info("startTime:{},endTime:{},count:{},加入队列中将进行拆分。",startTime,endTime,count);
                AtomicLong finallyCount = new AtomicLong(0);
//                最好不要用递归，这里递归可能导致栈内存溢出
                Queue<TimeNode> queue = new LinkedList<TimeNode>();
                queue.add(new TimeNode(startTime,endTime,this.sdf,true,null,null,count,null));
                TimeNode t;
                StringBuilder sb;
                //记录分割后的时间段的兄弟时间段的总数
                Map<String,Long> periodNum = new HashMap<>(128);
                while((t = queue.poll())!=null){
                    //如果程序中断，可使用工具恢复进度
                    log.info("当前处理：{} --> {}",t.getStartTime(),t.getEndTime());
                    sb = new StringBuilder();
                    for (TimeNode timeNode : queue) {
                        sb.append("\t").append(timeNode.getStartTime()).append(" --> ").append(timeNode.getEndTime()).append("\n");
                    }
                    log.info("待处理：\n{}",sb.toString());
                    sb = null;
                    //totalNum只有第一次有值，后面拆分的都没有值，后面的手动赋值在这里是拿不到的，可以将第一次的拆分放在外面
                    if(t.getIsCount() && t.getTotalNum() > maxQueryNum){
                        TimeNode[] splitTimeNode = t.split();
                        TimeNode left = splitTimeNode[0];
                        TimeNode right = splitTimeNode[1];
                        //在第二次及以后拆分的parentTotalNum为空，手动设置
                        left.setParentTotalNum(t.getTotalNum());
                        right.setParentTotalNum(t.getTotalNum());
                        queue.add(left);
                        queue.add(right);
                    }else{
                        if(t.getStartTime().equals(t.getEndTime())){
                            log.info("startTime:{},endTime:{},相同，size:0",t.getStartTime(), t.getEndTime());
                            continue;
                        }
                        String delimiter =  " -> ";
                        String periodNumKey = t.getBrotherStartTime() + delimiter + t.getBrotherEndTime();
                        Long brotherNum = periodNum.get(periodNumKey);
                        Long ct = 0L;
                        if(brotherNum == null){
                            String countTmp = StrUtil.format(this.sqlCount, t.getStartTime(), t.getEndTime());
                            ct = hiveJdbc.queryForObject(countTmp, Long.class);
                            periodNum.put(t.getStartTime() + delimiter + t.getEndTime(),ct);
                            t.setTotalNum(ct);
                        }else{
                            ct = t.getParentTotalNum()-brotherNum;
                            t.setTotalNum(ct);
                            log.info("startTime:{},endTime:{},获取到缓存size:{}",t.getStartTime(), t.getEndTime(),ct);
                            periodNum.remove(periodNumKey);
                        }

                        if(ct > maxQueryNum){
//                            因为采用的是二分拆法，当间隔小于2秒时，还是大于maxQueryNum，则采用分页方式拉取数据，否则可能导致OOM，这样的情况应该是少数
                            if( (t.endTime2Date().getTime() - t.startTime2Date().getTime()) < 2000 ){
                                long rounds = ((Double)Math.ceil(ct / (this.pageSize*1.0))).longValue();
                                long curPage = 1;
                                log.info("startTime:{},endTime:{}，size:{},使用时间已不能再拆分,采用分页方式.默认分页大小:200000,默认分页大小可通过配置pageSize参数修改",t.getStartTime(), t.getEndTime(),ct);
                                while(curPage <= rounds){
                                    long tmpLimit = curPage * this.pageSize;
                                    tmpLimit = tmpLimit>ct?ct:tmpLimit;
                                    log.info("startTime:{},endTime:{}，curPage:{},totalPages:{},pageSize:{}",t.getStartTime(), t.getEndTime(),curPage,rounds,this.pageSize);
                                    String sqlRowNum = StrUtil.format(this.sqlRowNum, t.getStartTime(), t.getEndTime(),tmpLimit,this.pageSize*(curPage-1)+1,tmpLimit);
                                    log.info("sql:{}",sqlRowNum);
                                    data = hiveJdbc.queryForList(sqlRowNum);
                                    data.forEach(item->{
                                        Record record = this.buildRecord(recordSender, item);
                                        recordSender.sendToWriter(record);
                                    });
                                    finallyCount.addAndGet(data.size());
                                    data.clear();
                                    curPage++;
                                }
                            }else{
                                log.info("startTime:{},endTime:{},count:{},加入队列中将进行拆分。",t.getStartTime(),t.getEndTime(),ct);
                                TimeNode[] splitTimeNode = t.split();
                                TimeNode left = splitTimeNode[0];
                                TimeNode right = splitTimeNode[1];
                                //在第二次及以后拆分的parentTotalNum为空，手动设置
                                left.setParentTotalNum(ct);
                                right.setParentTotalNum(ct);
                                queue.add(left);
                                queue.add(right);
//                                queue.add(new TimeNode(t.getStartTime(),t.getEndTime(),this.sdf,true));
                            }
                        }else{
                            if(new Long(0).equals(ct)){
                                log.info("startTime:{},endTime:{}，size:0",t.getStartTime(), t.getEndTime());
                                continue;
                            }
                            String sqlTmp = StrUtil.format(this.sql, t.getStartTime(), t.getEndTime());
                            log.info("sql:{}",sqlTmp);
                            data = hiveJdbc.queryForList(sqlTmp);
                            if(Objects.isNull(data) || data.isEmpty()){
                                continue;
                            }
                            data.forEach(item->{
                                Record record = this.buildRecord(recordSender, item);
                                recordSender.sendToWriter(record);
                            });
                            finallyCount.addAndGet(data.size());
                            data.clear();
                        }
                    }
                }

                return finallyCount.get();
            }else{
                String sqlTmp = StrUtil.format(this.sql, startTime, endTime);
                log.info("sql:{}",sqlTmp);
                data = hiveJdbc.queryForList(sqlTmp);
                if(Objects.isNull(data) || data.isEmpty()){
                    return 0;
                }
                data.forEach(item->{
                    Record record = this.buildRecord(recordSender, item);
                    recordSender.sendToWriter(record);
                });
                return data.size();
            }
        }

    }

    @Slf4j
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class TimeNode{
        private String startTime;
        private String endTime;
        private SimpleDateFormat sdf;
        private Boolean isCount = false;

        private String brotherStartTime;
        private String brotherEndTime;
        private Long totalNum;
        private Long parentTotalNum;

        public Date startTime2Date(){
            try {
                return sdf.parse(startTime);
            } catch (ParseException e) {
                e.printStackTrace();
                return null;
            }
        }

        public Date endTime2Date(){
            try {
                return sdf.parse(endTime);
            } catch (ParseException e) {
                e.printStackTrace();
                return null;
            }
        }

        public TimeNode[] split(){
            long time1 = this.startTime2Date().getTime();
            long time2 = this.endTime2Date().getTime();
            long t = (time2-time1)/2;
            TimeNode[] timeNodes = new TimeNode[2];
            String leftStartTime = this.sdf.format(new Date(time1));
            String leftEndTime = this.sdf.format(new Date(time1 + t));
            String rightStartTime = this.sdf.format(new Date(time2));
            timeNodes[0] = new TimeNode(leftStartTime,leftEndTime,this.sdf,false,leftEndTime,rightStartTime,null,this.totalNum);
            timeNodes[1] = new TimeNode(timeNodes[0].getEndTime(),rightStartTime,this.sdf,false,leftStartTime,leftEndTime,null,this.totalNum);
            return timeNodes;
        }

    }


}
