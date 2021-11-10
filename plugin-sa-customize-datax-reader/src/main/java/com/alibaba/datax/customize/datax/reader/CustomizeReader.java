package com.alibaba.datax.customize.datax.reader;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.customize.datax.classloader.PluginClassLoader;
import com.alibaba.datax.customize.datax.domain.SaPlugin;
import com.alibaba.datax.customize.datax.util.NullUtil;
import com.alibaba.datax.customize.datax.util.PluginUtil;
import com.alibaba.datax.reader.KeyConstant;
import com.alibaba.datax.reader.common.CommonReader;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

public class CustomizeReader extends Reader {

    @Slf4j
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Job extends Reader.Job{

        private Configuration originalConfig = null;

        @Override
        public List<Configuration> split(int i) {
            CommonReader.SAReaderPlugin plugin = PluginUtil.plugin();
            if(!Objects.isNull(plugin)){
                return plugin.splitJob(this.originalConfig,i);
            }
            List<Configuration> splitConfigs = new ArrayList<Configuration>();
            for (int j = 0; j < i; j++) {
                splitConfigs.add(this.originalConfig.clone());
            }
            return splitConfigs;
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            String saPluginStr = originalConfig.getString(KeyConstant.PLUGIN,null);
            if(NullUtil.isNullOrBlank(saPluginStr)){
                return;
            }
            SaPlugin saPlugin = JSONObject.parseObject(saPluginStr, SaPlugin.class);
            String pluginName = saPlugin.getName();
            String pluginClass = saPlugin.getClassName();
            Map<String, Object> pluginParam = saPlugin.getParam();
            if(!NullUtil.isNullOrBlank(pluginName) && !NullUtil.isNullOrBlank(pluginClass)){
                if(Objects.isNull(pluginParam)){
                    pluginParam = new HashMap<>();
                }
                PluginUtil.setPlugin(PluginClassLoader.getBasePlugin(saPlugin.getName(), pluginClass, this.originalConfig,pluginParam));
            }

            CommonReader.SAReaderPlugin plugin = PluginUtil.plugin();
            plugin.initJob(this.originalConfig);
        }

        @Override
        public void destroy() {
            CommonReader.SAReaderPlugin plugin = PluginUtil.plugin();
            if(!Objects.isNull(plugin)) {
                plugin.destroyJob(this.originalConfig);
            }
        }
    }


    @Slf4j
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Task extends Reader.Task {

        private Configuration readerConfig;
        /**
         * 列的简称
         */
        private List<String>  columnNameList;

        @Override
        public void startRead(RecordSender recordSender) {
            CommonReader.SAReaderPlugin plugin = PluginUtil.plugin();
            if(!Objects.isNull(plugin)) {
                plugin.startReadTask(recordSender, super.getTaskPluginCollector(),columnNameList);
            }
        }

        @Override
        public void init() {
            this.readerConfig = super.getPluginJobConf();
            this.columnNameList = readerConfig.getList(KeyConstant.COLUMN,new ArrayList<>(),String.class);
            CommonReader.SAReaderPlugin plugin = PluginUtil.plugin();
            if(!Objects.isNull(plugin)) {
                plugin.initTask(this.readerConfig);
            }
        }


        @Override
        public void destroy() {
            CommonReader.SAReaderPlugin plugin = PluginUtil.plugin();
            if(!Objects.isNull(plugin)) {
                plugin.destroyJob(this.readerConfig);
            }
        }
    }

}
