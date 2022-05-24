package com.alibaba.datax.plugin.reader.kafkareader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KafkaReader extends Reader {
    //kafka batchreader
    public static List<KafkaConsumer> consumerList = new ArrayList<KafkaConsumer>();


    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        private Configuration originalConfig = null;

        private Integer partitionsNum;
        private List<PartitionInfo> partitionInfos;

        //job共享cosumer 用于获取partition和提交offset
        private KafkaConsumer<String, String> consumer0;
        private String topic;
        private String type;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            // warn: 忽略大小写

            topic = this.originalConfig
                    .getString(Key.TOPIC);
//			ddddddddxddddreInteger partitions = this.originalConfig
//					.getInt(Key.KAFKA_PARTITIONS);
            String bootstrapServers = this.originalConfig
                    .getString(Key.BOOTSTRAP_SERVERS);

            String groupId = this.originalConfig.getString(Key.GROUP_ID);
            String split = this.originalConfig.getString(Key.SPLIT);
//            String filterContaintsStr = this.originalConfig.getString(Key.CONTAINTS_STR);
//            String filterContaintsFlag = this.originalConfig.getString(Key.CONTAINTS_STR_FLAG);
//            String conditionAllOrOne = this.originalConfig.getString(Key.CONDITION_ALL_OR_ONE);
            String parsingRules = this.originalConfig.getString(Key.PARSING_RULES);
            String writerOrder = this.originalConfig.getString(Key.WRITER_ORDER);
            String kafkaReaderColumnKey = this.originalConfig.getString(Key.KAFKA_READER_COLUMN_KEY);
            type = this.originalConfig.getString(Key.KAFKA_READ_TYPE);

            if (null == topic) {

                throw DataXException.asDataXException(KafkaReaderErrorCode.TOPIC_ERROR,
                        "没有设置参数[topic].");
            }
            if (null == bootstrapServers) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.ADDRESS_ERROR,
                        "没有设置参数[bootstrap.servers].");
            }
            if (null == groupId) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.KAFKA_READER_ERROR,
                        "没有设置参数[groupid].");
            }

//            if (filterContaintsStr != null) {
//                if (conditionAllOrOne == null || filterContaintsFlag == null) {
//                    throw DataXException.asDataXException(KafkaReaderErrorCode.KAFKA_READER_ERROR,
//                            "设置了[filterContaintsStr],但是没有设置[conditionAllOrOne]或者[filterContaintsFlag]");
//                }
//            }
            if (parsingRules == null) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.KAFKA_READER_ERROR,
                        "没有设置[parsingRules]参数");
            } else if (!parsingRules.equals("regex") && parsingRules.equals("json") && parsingRules.equals("split")) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.KAFKA_READER_ERROR,
                        "[parsingRules]参数设置错误，不是regex，json，split其中一个");
            }

            if (kafkaReaderColumnKey == null) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.KAFKA_READER_ERROR,
                        "没有设置[kafkaReaderColumnKey]参数");
            }
            if (groupId == null) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.KAFKA_READER_ERROR,
                        "没有设置[groupId]参数");
            }
            if (!type.equals("batch") && !type.equals("stream")) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.READ_TYPE_ERROR,
                        "请设置[type]参数为batch或stream");
            }


            //获取partitions 和task不互通
            Properties props = KafkaHelper.setProperties(originalConfig);
            List<Integer> partitionList = new ArrayList();
            // 定义consumer
            consumer0 = new KafkaConsumer<String, String>(props);
            // Determine which partitions to subscribe to, for now do all
            partitionInfos = consumer0.partitionsFor(topic);
            for (PartitionInfo partitionInfo : partitionInfos) {
                partitionList.add(partitionInfo.partition());
            }
            partitionsNum = partitionList.size();
        }


        @Override
        public void preCheck() {
            init();

        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> configurations = new ArrayList<Configuration>();
            //按照channel数切分
            int consumernum = adviceNumber;
            //按照分区数切分
            if (adviceNumber >= partitionsNum) {
                consumernum = partitionsNum;
            }
            int partitionCode[] = new int[partitionsNum];
            for (int i = 0; i < partitionsNum; i++) {
                partitionCode[i] = i;
            }
            int partitionsPerConsumer = (int) Math.round((double) partitionsNum / consumernum);

            for (int i = 0; i < consumernum; i++) {
                Configuration kafkaConfig = this.originalConfig.clone();
                //批量读取 手动分配partition
                if (type.equals("batch")) {
                    int start = i * partitionsPerConsumer;
                    int end = start + partitionsPerConsumer;
                    if (end > partitionsNum || (i == consumernum - 1 && end < partitionsNum)) {
                        end = partitionsNum;
                    }
                    int[] partitionsArr = ArrayUtils.subarray(partitionCode, start, end);
                    List<Integer> list = new ArrayList<Integer>();
                    for (int j = 0; j < partitionsArr.length; j++) {
                        list.add(partitionsArr[j]);
                    }
                    kafkaConfig.set("partitionList", list);
                }

                configurations.add(kafkaConfig);
            }


////			Integer partitions = this.originalConfig.getInt(Key.KAFKA_PARTITIONS);
//			int splitNnum;
//			//按照分区数切分
//			if (adviceNumber >= partitionsNum) {
//				splitNnum = partitionsNum;
//			} else {
//				//按照channel数切分
//				splitNnum = adviceNumber;
//			}
//			for (int i = 0; i < splitNnum; i++) {
//				Configuration kafkaConfig = this.originalConfig.clone();
//				kafkaConfig.set("partitionList",);
//				configurations.add(kafkaConfig);
//			}
            return configurations;
        }

        @Override
        public void post() {
            //batch等待写完提交offset
            if (type.equals("batch")) {
                for (KafkaConsumer consumer : consumerList) {
                    consumer.commitSync();
                }
                logOffset(consumerList.get(0), topic);
            }
        }

        @Override
        public void destroy() {

        }

        public void logOffset(KafkaConsumer consumer, String topic) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            List<TopicPartition> topicPartitions = new ArrayList<>();
            for (PartitionInfo partitionInfo : partitionInfos) {
                TopicPartition p1 = new TopicPartition(topic, partitionInfo.partition());
                topicPartitions.add(p1);
            }
            Map<TopicPartition, Long> endoffsets = consumer.endOffsets(topicPartitions);
            for (TopicPartition partition : topicPartitions) {
                LOG.info("partitionInfo:" + partition + "提交offset：" + endoffsets.get(partition));
            }

        }

    }

    public static class Task extends Reader.Task {

        private static final Logger LOG = LoggerFactory
                .getLogger(KafkaReader.Task.class);
        //配置文件
        private Configuration readerSliceConfig;
        //kafka消息的分隔符
        private String split;
        //解析规则
        private String parsingRules;
        //是否停止拉去数据
        private boolean flag;
        //kafka address
        private String bootstrapServers;
        //kafka groupid
        private String groupId;
        //kafkatopic
        private String kafkaTopic;
        //是否需要data_from
        //kafka ip 端口+ topic
        //将包含/不包含该字符串的数据过滤掉
//        private String filterContaintsStr;
        //是包含containtsStr 还是不包含
        //1 表示包含 0 表示不包含
//        private int filterContaintsStrFlag;
        //全部包含或不包含，包含其中一个或者不包含其中一个。
//        private int conditionAllOrOne;
        //writer端要求的顺序。
        private String writerOrder;
        //kafkareader端的每个关键子的key
        private String kafkaReaderColumnKey;
        //异常文件路径
        private String exceptionPath;
        //指定partitions
        private List<Integer> partitionList;
        //task共享cosumer
        private KafkaConsumer<String, String> consumer;
        //kafka读取方式
        private String type;
        //kafka一次消费超时时间，默认6s
        private long connsumerPollTimeout;
        //等待写入超时时间 默认10min
        private long waitTimeOut;

        String keyTab;
        String principal;
        Boolean isEarliest;

        @Override
        public void init() {
            flag = true;
            this.readerSliceConfig = super.getPluginJobConf();
            split = this.readerSliceConfig.getString(Key.SPLIT);
            bootstrapServers = this.readerSliceConfig.getString(Key.BOOTSTRAP_SERVERS);
            groupId = this.readerSliceConfig.getString(Key.GROUP_ID);
            kafkaTopic = this.readerSliceConfig.getString(Key.TOPIC);
//            filterContaintsStr = this.readerSliceConfig.getString(Key.CONTAINTS_STR);
//            filterContaintsStrFlag = this.readerSliceConfig.getInt(Key.CONTAINTS_STR_FLAG, 0);
//            conditionAllOrOne = this.readerSliceConfig.getInt(Key.CONTAINTS_STR_FLAG, 0);
            parsingRules = this.readerSliceConfig.getString(Key.PARSING_RULES);
            writerOrder = this.readerSliceConfig.getString(Key.WRITER_ORDER);
            kafkaReaderColumnKey = this.readerSliceConfig.getString(Key.KAFKA_READER_COLUMN_KEY);
            exceptionPath = this.readerSliceConfig.getString(Key.EXECPTION_PATH);
            partitionList = this.readerSliceConfig.getList("partitionList", Integer.class);
            type = this.readerSliceConfig.getString(Key.KAFKA_READ_TYPE);
            waitTimeOut = this.readerSliceConfig.getLong(Key.WAIT_TIMEOUT, 600000L);
            connsumerPollTimeout = this.readerSliceConfig.getLong(Key.CONNSUMER_POLL_TIMEOUT, 6000L);
            keyTab = this.readerSliceConfig.getString(Key.KAFKA_KERBEROS_KEYTAB);
            isEarliest = this.readerSliceConfig.getBool(Key.IS_EARLIEST, false);


//            LOG.info(filterContaintsStr);
        }

        @Override
        public void startRead(RecordSender recordSender) {
            Properties props = KafkaHelper.setProperties(readerSliceConfig);
            Record oneRecord = null;


            //批量读取，手动分配partitions，在job post提交offset
            if (type.equals("batch")) {
                consumer = new KafkaConsumer<String, String>(props);
                consumerList.add(consumer);
//			consumer.subscribe(Collections.singletonList(kafkaTopic));
                List<TopicPartition> partitions = new ArrayList<TopicPartition>();
                for (Integer partition : partitionList) {
                    partitions.add(new TopicPartition(kafkaTopic, partition));
                }
                consumer.assign(partitions);


                LOG.debug("Get the set of partitions currently assigned to this consumer: " + consumer.assignment());
                //todo batch批量提交 //todo 修改参数flag
                //读取总数量
                long totalReadNum = 0;
                Map<TopicPartition, Long> endoffsets = consumer.endOffsets(partitions);

                if (isEarliest) {
                    for (TopicPartition partition : partitions) {
                        consumer.seek(partition, 0);
                        totalReadNum = totalReadNum + endoffsets.get(partition) - 0;
                    }
                } else {
                    for (TopicPartition partition : partitions) {
                        totalReadNum = totalReadNum + endoffsets.get(partition) - consumer.position(partition);
                    }
                }
                LOG.info("即将读取数量：" + totalReadNum);
                int recordNumCount = 0;
                while (flag) {
                    LOG.debug("connsumerPollTimeout:" + connsumerPollTimeout);
                    ConsumerRecords<String, String> records = consumer.poll(connsumerPollTimeout);
                    for (ConsumerRecord<String, String> record : records) {

                        String value = record.value();
//                        //定义过滤标志
//                        int ifNotContinue = filterMessage(value);
//                        //如果标志修改为1了那么就过滤掉这条数据。
//                        if (ifNotContinue == 1) {
//                            LOG.info("过滤数据： " + record.value());
//                            continue;
//                        }
                        oneRecord = buildOneRecord(recordSender, value);
                        //如果返回值不等于null表示不是异常消息。
                        if (oneRecord != null) {
                            recordSender.sendToWriter(oneRecord);
                        }
                    }
                    recordNumCount = recordNumCount + records.count();
                    if (recordNumCount >= totalReadNum) {
                        flag = false;
                    }
                }
            }

            //todo 流读取，自动分配partitions，在task startRead 提交offset
            if (type.equals("stream")) {
                KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
                consumer.subscribe(Collections.singletonList(kafkaTopic));
                LOG.debug("===stream Get the set of partitions currently assigned to this consumer: " + consumer.assignment());
                while (flag) {
                    LOG.debug("connsumerPollTimeout:" + connsumerPollTimeout);
                    ConsumerRecords<String, String> records = consumer.poll(connsumerPollTimeout);
                    for (ConsumerRecord<String, String> record : records) {
                        String value = record.value();
//                        //定义过滤标志
//                        int ifNotContinue = filterMessage(value);
//                        //如果标志修改为1了那么就过滤掉这条数据。
//                        if (ifNotContinue == 1) {
//                            LOG.info("过滤数据： " + record.value());
//                            continue;
//                        }
                        oneRecord = buildOneRecord(recordSender, value);
                        //如果返回值不等于null表示不是异常消息。
                        if (oneRecord != null) {
                            recordSender.sendToWriter(oneRecord);
                            LOG.debug("***********提交offset******************");
                            consumer.commitSync();
                        }
                    }
                    //poll一次等待channel清空
                    if (waitTimeOut > 0) {
                        recordSender.flush(waitTimeOut);
                    } else {
                        recordSender.flush();
                    }

                }
                LOG.debug("========consumer.poll finish=========");
                recordSender.flush();
                LOG.debug("***********提交offset******************");
                logOffset(consumer, kafkaTopic);
                consumer.commitSync();
            }


            //打印结束startread
            LOG.info("----------------Finished read record--------");


            //最后手动flush buffer数据到channel
            recordSender.flush();
        }

//        private int filterMessage(String value) {
//            //如果要过滤的条件配置了
//            int ifNotContinue = 0;
//
//            if (filterContaintsStr != null) {
//                String[] filterStrs = filterContaintsStr.split(",");
//                //所有
//                if (conditionAllOrOne == 1) {
//                    //过滤掉包含filterContaintsStr的所有项的值。
//                    if (filterContaintsStrFlag == 1) {
//                        int i = 0;
//                        for (; i < filterStrs.length; i++) {
//                            if (!value.contains(filterStrs[i])) break;
//                        }
//                        if (i >= filterStrs.length) ifNotContinue = 1;
//                    } else {
//                        //留下掉包含filterContaintsStr的所有项的值
//                        int i = 0;
//                        for (; i < filterStrs.length; i++) {
//                            if (!value.contains(filterStrs[i])) break;
//                        }
//                        if (i < filterStrs.length) ifNotContinue = 1;
//                    }
//
//                } else {
//                    //过滤掉包含其中一项的值
//                    if (filterContaintsStrFlag == 1) {
//                        int i = 0;
//                        for (; i < filterStrs.length; i++) {
//                            if (value.contains(filterStrs[i])) break;
//                        }
//                        if (i < filterStrs.length) ifNotContinue = 1;
//                    }
//                    //留下包含其中一下的值
//                    else {
//                        int i = 0;
//                        for (; i < filterStrs.length; i++) {
//                            if (value.contains(filterStrs[i])) break;
//                        }
//                        if (i >= filterStrs.length) ifNotContinue = 1;
//                    }
//                }
//            }
//            return ifNotContinue;
//
//        }

        private Record buildOneRecord(RecordSender recordSender, String value) {
            Record record = null;
            if (parsingRules.equals("regex")) {
                record = parseRegex(value, recordSender);
            } else if (parsingRules.equals("json")) {
                record = parseJson(value, recordSender);
            } else if (parsingRules.equals("split")) {
                record = parseSplit(value, recordSender);
            }
            return record;
        }

        private Record parseSplit(String value, RecordSender recordSender) {
            if (null == split) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.KAFKA_READER_ERROR,
                        "[split]不能为空.");
            }
            Record record = recordSender.createRecord();
            String[] splits = value.split(this.split);
            parseOrders(Arrays.asList(splits), record);
            return record;
        }

        private Record parseJson(String value, RecordSender recordSender) {
            Record record = recordSender.createRecord();

            DocumentContext context = JsonPath.parse(value);
            String[] columns = kafkaReaderColumnKey.split(",");
            ArrayList<String> datas = new ArrayList<String>();
            for (String column : columns) {
                if (context.read("$." + column) == null) {
                    throw DataXException.asDataXException(KafkaReaderErrorCode.COLUMN_ERROR,
                            String.format("列信息[%s]有误", column));//todo 错误列一定退出？
                }
                datas.add(context.read("$." + column).toString());
            }
            parseOrders(datas, record);
            return record;
        }

        private Record parseRegex(String value, RecordSender recordSender) {
            Record record = recordSender.createRecord();
            ArrayList<String> datas = new ArrayList<String>();
            Pattern r = Pattern.compile(split);
            Matcher m = r.matcher(value);
            if (m.find()) {
                for (int i = 1; i <= m.groupCount(); i++) {
                    //  record.addColumn(new StringColumn(m.group(i)));
                    datas.add(m.group(i));
                    return record;
                }
            } else {
                writerErrorPath(value);
            }

            parseOrders(datas, record);

            return null;
        }

        private void writerErrorPath(String value) {
            if (exceptionPath == null) return;
            FileOutputStream fileOutputStream = null;
            try {
                fileOutputStream = getFileOutputStream();
                fileOutputStream.write((value + "\n").getBytes());
                fileOutputStream.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private FileOutputStream getFileOutputStream() throws FileNotFoundException {
            return new FileOutputStream(exceptionPath + "/" + kafkaTopic + "errordata" + DateUtil.targetFormat(new Date(), "yyyyMMdd"), true);
        }

        private void parseOrders(List<String> datas, Record record) {
            //writerOrder
            String[] orders = writerOrder.split(",");
            for (String order : orders) {
                if (order.equals("data_from")) {
                    record.addColumn(new StringColumn(bootstrapServers + "|" + kafkaTopic));
                } else if (order.equals("uuid")) {
                    record.addColumn(new StringColumn(UUID.randomUUID().toString()));
                } else if (order.equals("null")) {
                    record.addColumn(new StringColumn("null"));
                } else if (order.equals("datax_time")) {
                    record.addColumn(new StringColumn(DateUtil.targetFormat(new Date())));
                } else if (isNumeric(order)) {
                    record.addColumn(new StringColumn(datas.get(new Integer(order) - 1)));
                }
            }
        }

        public static boolean isNumeric(String str) {
            for (int i = 0; i < str.length(); i++) {
                if (!Character.isDigit(str.charAt(i))) {
                    return false;
                }
            }
            return true;
        }

        public void logOffset(KafkaConsumer consumer, String topic) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            List<TopicPartition> topicPartitions = new ArrayList<>();
            for (PartitionInfo partitionInfo : partitionInfos) {
                TopicPartition p1 = new TopicPartition(topic, partitionInfo.partition());
                topicPartitions.add(p1);
            }
            Map<TopicPartition, Long> endoffsets = consumer.endOffsets(topicPartitions);
            for (TopicPartition partition : topicPartitions) {
                LOG.info("partitionInfo:" + partition + "提交offset：" + endoffsets.get(partition));
            }

        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
            flag = false;

        }
    }

}
