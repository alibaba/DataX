package com.alibaba.datax.plugin.reader.elasticsearchreader;


import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.alibaba.datax.common.element.*;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;


public class ESReader extends Reader {
    private static final org.slf4j.Logger logger = LoggerFactory
            .getLogger(ESReader.class);
    public static class Job extends Reader.Job {



        private String esClusterName = null;

        private String esClusterIP = null;

        private String esClusterPort = null;
        private String esUsername = null;
        private String esPassword = null;

        private String esIndex = null;
        private JSONArray esColumnMeta = null;
        private String esType = null;



        private String query = null;

//        private TransportClient client = null;

        private Integer batchSize = 1000;
        private EsReaderUtil esResultSet = null;


        private Configuration readerSplitConfiguration = null;

        @Override
        public void preCheck() {
            super.preCheck();
        }

        @Override
        public void preHandler(Configuration jobConfiguration) {
            super.preHandler(jobConfiguration);
        }

        @Override
        public void init() {
            this.readerSplitConfiguration = super.getPluginJobConf();



            this.esColumnMeta = JSON.parseArray(readerSplitConfiguration.getString(Key.esColumn));
            this.esClusterName = this.readerSplitConfiguration.getString(Key.esClusterName);
            this.esClusterIP = readerSplitConfiguration.getString(Key.esClusterIP);
            this.esClusterPort = readerSplitConfiguration.getString(Key.esClusterPort, "9300");
            this.esIndex = readerSplitConfiguration.getString(Key.esIndex);
            this.esType = readerSplitConfiguration.getString(Key.esType);
            this.batchSize = readerSplitConfiguration.getInt(Key.batchSize, 1000);
            this.esUsername = readerSplitConfiguration.getString(Key.esUsername);
            this.esPassword = readerSplitConfiguration.getString(Key.esPassword);
            this.query = readerSplitConfiguration.getString(Key.query);
        }

        @Override
        public void prepare() {
            super.prepare();

            this.esResultSet = new EsReaderUtil(this.esUsername,this.esPassword ,this.esClusterIP,this.esClusterPort,this.batchSize);
        }

        @Override
        public void post() {
            super.post();
        }

        @Override
        public void postHandler(Configuration jobConfiguration) {
            super.postHandler(jobConfiguration);
        }

        @Override
        public void destroy() {
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> readerSplitConfigurations = new ArrayList<Configuration>();
            for (int i = 0; i < adviceNumber; i++) {
                Configuration readerSplitConfiguration = this.readerSplitConfiguration.clone();
                readerSplitConfigurations.add(readerSplitConfiguration);
            }
            return readerSplitConfigurations;
        }

    }

    public static class Task extends Reader.Task {

        private Configuration readerSplitConfiguration = null;

        private String esClusterName = null;

        private String esClusterIP = null;

        private String esClusterPort = null;
        private String esUsername = null;
        private String esPassword = null;

        private String esIndex = null;
        private JSONArray esColumnMeta = null;
        private String esType = null;



        private String query = null;

//        private TransportClient client = null;

        private Integer batchSize = 1000;
        private EsReaderUtil esResultSet = null;

        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        @Override
        public void preCheck() {
            super.preCheck();
        }

        @Override
        public void preHandler(Configuration jobConfiguration) {
            super.preHandler(jobConfiguration);
        }

        @Override
        public void init() {
            this.readerSplitConfiguration= super.getPluginJobConf();


            this.esColumnMeta = JSON.parseArray(readerSplitConfiguration.getString(Key.esColumn));
            this.esClusterName = this.readerSplitConfiguration.getString(Key.esClusterName);
            this.esClusterIP = readerSplitConfiguration.getString(Key.esClusterIP);
            this.esClusterPort = readerSplitConfiguration.getString(Key.esClusterPort, "9300");
            this.esIndex = readerSplitConfiguration.getString(Key.esIndex);
            this.esType = readerSplitConfiguration.getString(Key.esType);
            this.batchSize = readerSplitConfiguration.getInt(Key.batchSize, 100);
            this.esUsername = readerSplitConfiguration.getString(Key.esUsername);
            this.esPassword = readerSplitConfiguration.getString(Key.esPassword);
            this.query = readerSplitConfiguration.getString(Key.query);
        }

        @Override
        public void prepare() {
            super.prepare();
            this.esResultSet = new EsReaderUtil(this.esUsername,this.esPassword ,this.esClusterIP,this.esClusterPort,this.batchSize);
        }

        @Override
        public void post() {
            super.post();
        }

        @Override
        public void postHandler(Configuration jobConfiguration) {
            super.postHandler(jobConfiguration);
        }

        @Override
        public void destroy() {
            //TODO 这里可以将游标进行销毁
//            client.close();
        }

        @Override
        public void startRead(RecordSender recordSender) {
            EsReaderUtil iterator = esResultSet.search(esIndex,esType,this.query);
            int totalSize = 0;


            while(iterator.hasNext()){
                Record record = recordSender.createRecord();
                JSONObject line = (JSONObject)iterator.next().get("_source");
//                logger.debug(line.toJSONString());
                Iterator columnItera = esColumnMeta.iterator();
                while (columnItera.hasNext()) {
                    JSONObject column = (JSONObject)columnItera.next();

                    Object tempCol = line.get(column.getString(Key.COLUMN_NAME));
                    if(tempCol==null) {
                        record.addColumn(new StringColumn(""));
                    }else if (tempCol instanceof JSONObject) {
                        JSONObject jsonObject = (JSONObject) tempCol;
                        record.addColumn(new StringColumn(jsonObject.toJSONString()));
                    }else if (tempCol instanceof JSONArray) {
                        JSONArray jsonArray = (JSONArray) tempCol;
                        record.addColumn(new StringColumn(jsonArray.toJSONString()));
                    }else if (tempCol instanceof Double) {
                        record.addColumn(new DoubleColumn((Double) tempCol));
                    } else if (tempCol instanceof Boolean) {
                        record.addColumn(new BoolColumn((Boolean) tempCol));
                    } else if (tempCol instanceof Date) {
                        record.addColumn(new DateColumn((Date) tempCol));
                    } else if (tempCol instanceof Integer) {
                        record.addColumn(new LongColumn((Integer) tempCol));
                    }else if (tempCol instanceof Long) {
                        record.addColumn(new LongColumn((Long) tempCol));
                    }else if(tempCol instanceof BigDecimal){
                        record.addColumn(new DoubleColumn((BigDecimal)tempCol));
                    }else {
                        record.addColumn(new StringColumn((String) tempCol));
                    }

                }
                recordSender.sendToWriter(record);

                totalSize++;

            }


        }

    }

}
