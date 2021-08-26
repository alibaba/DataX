package com.alibaba.datax.plugin.writer;

import cn.sensorsdata.BasePlugin;
import com.alibaba.datax.plugin.domain.SaPlugin;
import com.alibaba.datax.plugin.util.ConverterUtil;
import com.alibaba.datax.plugin.util.SaUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.datax.plugin.ConverterFactory;
import com.alibaba.datax.plugin.KeyConstant;
import com.alibaba.datax.plugin.classloader.PluginClassLoader;
import com.alibaba.datax.plugin.domain.DataConverter;
import com.alibaba.datax.plugin.domain.SaColumnItem;
import com.alibaba.datax.plugin.util.NullUtil;
import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sensorsdata.analytics.javasdk.SensorsAnalytics;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

public class SaWriter extends Writer {

    @Slf4j
    public static class Job extends Writer.Job{

        private Configuration originalConfig = null;

        public List<Configuration> split(int i) {
            List<Configuration> list = new ArrayList<>();
            for (int j = 0; j < i; j++) {
                list.add(this.originalConfig.clone());
            }
            return list;
        }

        public void init() {
            this.originalConfig = super.getPluginJobConf();
            String sdkDataAddress = originalConfig.getString(KeyConstant.SDK_DATA_ADDRESS);
            if(StrUtil.isBlank(sdkDataAddress)){
                throw new DataXException(CommonErrorCode.CONFIG_ERROR,"sdkDataAddress不能为空");
            }
            SaUtil.setSdkDataAddress(sdkDataAddress);
            try {
                boolean isGenerateLog = originalConfig.getBool(KeyConstant.IS_GENERATE_LOG);
                if(!Objects.isNull(isGenerateLog)){
                    SaUtil.setIsGenerateLog(isGenerateLog);
                }
            }catch (Exception e){
                log.info("isGenerateLog未配置，使用默认值：true");
            }

            String type = originalConfig.getString(KeyConstant.TYPE);
            JSONArray saColumnJsonArray = originalConfig.get(KeyConstant.SA_COLUMN, JSONArray.class);
            String saColumnStr = saColumnJsonArray.toJSONString();
            List<SaColumnItem> saColumnList = JSONObject.parseArray(saColumnStr, SaColumnItem.class);
            if(Objects.isNull(saColumnList) || saColumnList.isEmpty()){
                throw new DataXException(CommonErrorCode.CONFIG_ERROR,"saColumn不应该为空！");
            }
            List<String> saColumnNameList = saColumnList.stream().map(SaColumnItem::getTargetColumnName).collect(Collectors.toList());
            if(KeyConstant.TRACK.equalsIgnoreCase(type)){
                String distinctId = originalConfig.getString(KeyConstant.TRACK.concat(KeyConstant.POINT).concat(KeyConstant.DISTINCT_ID_COLUMN));
                Boolean isLoginId = originalConfig.getBool(KeyConstant.TRACK.concat(KeyConstant.POINT).concat(KeyConstant.IS_LOGIN_ID));
                String eventName = originalConfig.getString(KeyConstant.TRACK.concat(KeyConstant.POINT).concat(KeyConstant.EVENT_NAME));
                if(StrUtil.isBlank(distinctId) || Objects.isNull(isLoginId) ||
                        StrUtil.isBlank(eventName) || !saColumnNameList.contains(distinctId)){
                    throw new DataXException(CommonErrorCode.CONFIG_ERROR,"type为:track时，track属性配置错误或者distinctIdColumn属性未在saColumn中.");
                }
            }else if(KeyConstant.USER.equalsIgnoreCase(type)){
                String distinctId = originalConfig.getString(KeyConstant.USER.concat(KeyConstant.POINT).concat(KeyConstant.DISTINCT_ID_COLUMN));
                Boolean isLoginId = originalConfig.getBool(KeyConstant.USER.concat(KeyConstant.POINT).concat(KeyConstant.IS_LOGIN_ID));
                if(StrUtil.isBlank(distinctId) || Objects.isNull(isLoginId) ||  !saColumnNameList.contains(distinctId)){
                    throw new DataXException(CommonErrorCode.CONFIG_ERROR,"type为:user时，user属性配置错误或者distinctIdColumn属性未在saColumn中.");
                }
            }else if(KeyConstant.ITEM.equalsIgnoreCase(type)){
                String itemType = originalConfig.getString(KeyConstant.ITEM.concat(KeyConstant.POINT).concat(KeyConstant.ITEM_TYPE));
                Boolean typeIsColumn = originalConfig.getBool(KeyConstant.ITEM.concat(KeyConstant.POINT).concat(KeyConstant.TYPE_IS_COLUMN));
                String itemIdColumn = originalConfig.getString(KeyConstant.ITEM.concat(KeyConstant.POINT).concat(KeyConstant.ITEM_ID_COLUMN));
                if(StrUtil.isBlank(itemType) || Objects.isNull(typeIsColumn) ||
                        StrUtil.isBlank(itemIdColumn) || !saColumnNameList.contains(itemIdColumn) || (typeIsColumn && !saColumnNameList.contains(itemType))){
                    throw new DataXException(CommonErrorCode.CONFIG_ERROR,"type为:item时，item属性配置错误或者itemType或者itemIdColumn属性未在saColumn中.若typeIsColumn为false，itemType可以不在saColumn中");
                }
            }else{
                throw new DataXException(CommonErrorCode.CONFIG_ERROR,"不支持的type类型");
            }

        }

        public void destroy() {
            SaUtil.getInstance().shutdown();
        }
    }

    public static class Task extends Writer.Task{

        private static SensorsAnalytics sa ;

        private Configuration readerConfig;

        private String type;

        private List<SaColumnItem> saColumnList;

        private Map<String,Object> trackBaseProp = new HashMap<>();
        private Map<String,Object> userBaseProp = new HashMap<>();
        private Map<String,Object> itemBaseProp = new HashMap<>();

        private List<BasePlugin.SAPlugin> basePluginList;

        public void startWrite(RecordReceiver recordReceiver) {
            Record record = null;
            A:while((record = recordReceiver.getFromReader()) != null) {
                Map<String,Object> properties = new HashMap<>();

                if(KeyConstant.TRACK.equalsIgnoreCase(type)){
                    properties.putAll(trackBaseProp);
                }else if(KeyConstant.USER.equalsIgnoreCase(type)){
                    properties.putAll(userBaseProp);
                }else if(KeyConstant.ITEM.equalsIgnoreCase(type)){
                    properties.putAll(itemBaseProp);
                }else{
                    continue;
                }
                for (SaColumnItem col : saColumnList) {
                    Column column = record.getColumn(col.getIndex());
                    if(column instanceof StringColumn){
                        String v = column.asString();
                        Object value = ConverterUtil.convert(col.getTargetColumnName(),v,col,properties);
                        if(NullUtil.isNullOrBlank(value)){
                            if(!NullUtil.isNullOrBlank(col.getIfNullGiveUp()) && col.getIfNullGiveUp()){
                                continue A;
                            }
                            continue;
                        }
                        properties.put(col.getTargetColumnName(),value);
                    }else if(column instanceof BoolColumn){
                        Boolean v = column.asBoolean();
                        Object value = ConverterUtil.convert(col.getTargetColumnName(),v,col,properties);
                        if(NullUtil.isNullOrBlank(value)){
                            if(!NullUtil.isNullOrBlank(col.getIfNullGiveUp()) && col.getIfNullGiveUp()){
                                continue A;
                            }
                            continue;
                        }
                        properties.put(col.getTargetColumnName(),value);
                    }else if(column instanceof DoubleColumn){
                        BigDecimal v = column.asBigDecimal();
                        Object value = ConverterUtil.convert(col.getTargetColumnName(),v,col,properties);
                        if(NullUtil.isNullOrBlank(value)){
                            if(!NullUtil.isNullOrBlank(col.getIfNullGiveUp()) && col.getIfNullGiveUp()){
                                continue A;
                            }
                            continue;
                        }
                        properties.put(col.getTargetColumnName(),value);
                    }else if(column instanceof LongColumn){
                        BigInteger v = column.asBigInteger();
                        Object value = ConverterUtil.convert(col.getTargetColumnName(),v,col,properties);
                        if(NullUtil.isNullOrBlank(value)){
                            if(!NullUtil.isNullOrBlank(col.getIfNullGiveUp()) && col.getIfNullGiveUp()){
                                continue A;
                            }
                            continue;
                        }
                        properties.put(col.getTargetColumnName(),value);
                    }else if(column instanceof DateColumn){
                        Date v = column.asDate();
                        Object value = ConverterUtil.convert(col.getTargetColumnName(),v,col,properties);
                        if(NullUtil.isNullOrBlank(value)){
                            if(!NullUtil.isNullOrBlank(col.getIfNullGiveUp()) && col.getIfNullGiveUp()){
                                continue A;
                            }
                            continue;
                        }
                        properties.put(col.getTargetColumnName(),value);
                    }
                }
                boolean process = true;
                if(!Objects.isNull(this.basePluginList) && !this.basePluginList.isEmpty()){
                    for (BasePlugin.SAPlugin saPlugin : this.basePluginList) {
                        process = saPlugin.process(properties);
                        if(!process){
                            continue A;
                        }
                    }
                }
                SaUtil.process(sa,type,properties);
            }
        }

        public void init() {
            this.readerConfig = super.getPluginJobConf();
            this.type = readerConfig.getString(KeyConstant.TYPE);

            JSONArray saColumnJsonArray = readerConfig.get(KeyConstant.SA_COLUMN, JSONArray.class);
            String saColumnStr = saColumnJsonArray.toJSONString();
            this.saColumnList = JSONObject.parseArray(saColumnStr, SaColumnItem.class);
            if(Objects.isNull(saColumnList) || saColumnList.isEmpty()){
                throw new DataXException(CommonErrorCode.CONFIG_ERROR,"saColumn不应该为空！");
            }
            for (SaColumnItem col : saColumnList) {
                List<DataConverter> dataConverters = col.getDataConverters();
                if(Objects.isNull(dataConverters) || dataConverters.isEmpty()){
                    continue;
                }
                dataConverters.forEach(con->{
                    con.setConverter(ConverterFactory.converter(con.getType()));
                });
            }

            this.sa = SaUtil.getInstance();

            if(KeyConstant.TRACK.equalsIgnoreCase(type)){
                String eventDistinctIdCol = readerConfig.getString(KeyConstant.TRACK.concat(KeyConstant.POINT).concat(KeyConstant.DISTINCT_ID_COLUMN));
                Boolean eventIsLoginId = readerConfig.getBool(KeyConstant.TRACK.concat(KeyConstant.POINT).concat(KeyConstant.IS_LOGIN_ID));
                String eventEventName = readerConfig.getString(KeyConstant.TRACK.concat(KeyConstant.POINT).concat(KeyConstant.EVENT_NAME));
                trackBaseProp.put(KeyConstant.EVENT_DISTINCT_ID_COL,eventDistinctIdCol);
                trackBaseProp.put(KeyConstant.EVENT_IS_LOGIN_ID,eventIsLoginId);
                trackBaseProp.put(KeyConstant.EVENT_EVENT_NAME,eventEventName);
            }else if(KeyConstant.USER.equalsIgnoreCase(type)){
                String userDistinctId = readerConfig.getString(KeyConstant.USER.concat(KeyConstant.POINT).concat(KeyConstant.DISTINCT_ID_COLUMN));
                Boolean userIsLoginId = readerConfig.getBool(KeyConstant.USER.concat(KeyConstant.POINT).concat(KeyConstant.IS_LOGIN_ID));
                userBaseProp.put(KeyConstant.USER_DISTINCT_ID,userDistinctId);
                userBaseProp.put(KeyConstant.user_is_login_id,userIsLoginId);
            }else if(KeyConstant.ITEM.equalsIgnoreCase(type)){
                String itemItemType = readerConfig.getString(KeyConstant.ITEM.concat(KeyConstant.POINT).concat(KeyConstant.ITEM_TYPE));
                Boolean itemTypeIsColumn = readerConfig.getBool(KeyConstant.ITEM.concat(KeyConstant.POINT).concat(KeyConstant.TYPE_IS_COLUMN));
                String itemItemIdColumn = readerConfig.getString(KeyConstant.ITEM.concat(KeyConstant.POINT).concat(KeyConstant.ITEM_ID_COLUMN));
                itemBaseProp.put(KeyConstant.ITEM_ITEM_TYPE,itemItemType);
                itemBaseProp.put(KeyConstant.ITEM_TYPE_IS_COLUMN,itemTypeIsColumn);
                itemBaseProp.put(KeyConstant.ITEM_ITEM_ID_COLUMN,itemItemIdColumn);
            }

            String SaPluginStr = readerConfig.getString(KeyConstant.PLUGIN,"[]");
            List<SaPlugin> SaPluginList = JSONObject.parseArray(SaPluginStr, SaPlugin.class);
            if(!Objects.isNull(SaPluginList) && !SaPluginList.isEmpty()){
                basePluginList = new ArrayList<>();
            }

            SaPluginList.forEach(saPlugin -> {
                String pluginName = saPlugin.getName();
                String pluginClass = saPlugin.getClassName();
                Map<String, Object> pluginParam = saPlugin.getParam();
                if(!NullUtil.isNullOrBlank(pluginName) && !NullUtil.isNullOrBlank(pluginClass)){
                    if(Objects.isNull(pluginParam)){
                        pluginParam = new HashMap<>();
                    }
                    basePluginList.add(PluginClassLoader.getBasePlugin(saPlugin.getName(), pluginClass, pluginParam));
                }

            });
        }

        public void destroy() {}
    }


}
