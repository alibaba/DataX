package com.alibaba.datax.reader.common;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.reader.ReaderErrorCode;
import com.alibaba.datax.reader.util.TypeUtil;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

public abstract class CommonReader implements Serializable {

    private static final long serialVersionUID = 1507396893608235961L;

    /**
     * 获取SAReaderPlugin实例的方法
     * @param config DataX的原始配置类
     * @param param plugin.param下的参数
     * @return SAReaderPlugin实例
     */
    public abstract SAReaderPlugin instance(Configuration config, Map<String,Object> param);

    public abstract class SAReaderPlugin implements Serializable {

        private static final long serialVersionUID = 6613895888041966579L;

        /**
         * DataX job中 split 生命周期
         * @param config DataX的原始配置类
         * @param i DataX建议的分隔数
         * @return 分隔量，并发量
         */
        public List<Configuration> splitJob(Configuration config, int i) {
            List<Configuration> splitConfigs = new ArrayList<Configuration>();
            for (int j = 0; j < i; j++) {
                splitConfigs.add(config.clone());
            }
            return splitConfigs;
        }

        /**
         * DataX job中 init 生命周期
         * @param config DataX的原始配置类
         */
        public void initJob(Configuration config) {}

        /**
         * DataX job中 destroy 生命周期
         * @param config DataX的原始配置类
         */
        public void destroyJob(Configuration config) {}

        /**
         * 自定义读取数据入口
         * @param recordSender 行数据发送器
         * @param taskPluginCollector  错误数据收集器
         * @param columnNameList 发送到写插件的列名
         */
        public abstract void startReadTask(RecordSender recordSender,TaskPluginCollector taskPluginCollector,List<String>  columnNameList);

        /**
         * DataX task中 init 生命周期
         * @param config DataX的原始配置类
         */
        public void initTask(Configuration config) {}

        /**
         * DataX task中 destroy 生命周期
         * @param config DataX的原始配置类
         */
        public void destroyTask(Configuration config) {}

        /**
         * 默认的构建DataX当前行数据
         * @param taskPluginCollector 错误数据收集器
         * @param recordSender 行数据发送器
         * @param values 当前行所有数据
         * @param columnNameList 发送到写插件的列名
         * @return DataX一行数据
         */
        public Record buildRecord(TaskPluginCollector taskPluginCollector,RecordSender recordSender, Map<String, Object> values,List<String>  columnNameList){
            if(Objects.isNull(values) || values.isEmpty() || Objects.isNull(columnNameList) || columnNameList.isEmpty()){
                return null;
            }
            Record record = recordSender.createRecord();
            for (int index = 0; index < columnNameList.size(); index++) {
                String columnName = columnNameList.get(index);
                Object value = values.getOrDefault(columnName,null);
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
                }else if ((value instanceof Byte[]) || value instanceof byte[]) {
                    record.setColumn(index,new BytesColumn( (byte[])value) );
                }
                else{
                    this.addColumn(taskPluginCollector,record,index,value);
                }
            }
            return record;
        }

        /**
         * 处理默认未支持的数据类型转换
         * @param taskPluginCollector 错误数据收集器
         * @param record 以构建的当前行记录
         * @param index 当前列值应该在当前行中的位置下标，从零开始
         * @param value 当前列值
         */
        public void addColumn(TaskPluginCollector taskPluginCollector, Record record, int index, Object value){
            DataXException dataXException = DataXException
                    .asDataXException(
                            ReaderErrorCode.UNSUPPORTED_TYPE, String.format("不支持的数据类型type:%s,value:%s", value.getClass().getName(), value));
            taskPluginCollector.collectDirtyRecord(record,dataXException);
        }

    }


}
