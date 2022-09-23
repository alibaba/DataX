package writer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import org.apache.hadoop.util.Time;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.BooleanDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.StringDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class IoTDBTsFileWriter extends Writer {

    public static class Job extends Writer.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originalConfig;

        @Override
        public void init() {
            originalConfig = super.getPluginJobConf();
        }

        @Override
        public void destroy() {

        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            ArrayList<Configuration> configurations = new ArrayList<Configuration>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(originalConfig.clone());
            }
            return configurations;
        }
    }

    public static class Task extends Writer.Task {

        private static final Logger LOG = LoggerFactory.getLogger(Task.class);


        private String storeGroup = "root.ln";

        private String baseDir = "data";

        private long threshold = 2000;

        private final static String separator = ".";

        private final static String FILE_NAME_SEPARATOR = "-";

        private int vsgNum = 4;

        private final static int timeIndex = 0;

        private final static int valueIndex = 1;

        private final static int fieldIndex = 2;

        private final static int measurementIndex = 3;

        private final static int tagStartIndex = 5;

        private final Set devices = new HashSet();

        private final ConcurrentMap<Integer, TsFileWriter> writers = new ConcurrentHashMap<>();

        @Override
        public void init() {
            Configuration writerSliceConfig = getPluginJobConf();
            this.vsgNum = writerSliceConfig.getInt("vsgNum", 1);
            this.storeGroup = writerSliceConfig.getString("storeGroup", "root.influx");
            this.baseDir = writerSliceConfig.getString("baseDir", "data");
            this.threshold = writerSliceConfig.getLong("tsfileThresholdM", 2000) * 1000 * 1000;
        }

        @Override
        public void destroy() {
            // writer end file
            for (TsFileWriter writer : writers.values()) {
                try {
                    writer.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            Record record;
            long total = 0;
            while ((record = recordReceiver.getFromReader()) != null) {
                try {
                    writeWithRecord(record);
                } catch (Exception e) {
                    LOG.error("write with record error ", e);
                }
                total ++;
            }
            getTaskPluginCollector().collectMessage("write size", total + "");
            LOG.info("Task finished, write size: {}", total);
        }


        public static void main(String[] args) {
            /**
             * [_time值,_value值,_field值,_measurement值,Tag1Key,Tag1Value,
             * Tag2Key, Tag2Value, ......,TagNKey, TagNValue]
             * 从第一个Tag的value开始
             */
            Task task = new Task();
            for (int i = 0; i < 10000; i++) {
                DefaultRecord record = new DefaultRecord();
                record.addColumn(new LongColumn(Time.now()));
                record.addColumn(new LongColumn(i));
                record.addColumn(new StringColumn("value"));
                record.addColumn(new StringColumn("temperature"));
                record.addColumn(new StringColumn("location"));
                record.addColumn(new StringColumn("north"));
                try {
                    task.writeWithRecord(record);
                    TimeUnit.MILLISECONDS.sleep(1);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            }
            task.destroy();
        }

        private void writeWithRecord(Record record) throws Exception {

            long time = record.getColumn(timeIndex).asLong();
            String deviceId = getDeviceFullPath(record);
            int vsgIndex = deviceId.hashCode() % vsgNum;
            String measurement = record.getColumn(fieldIndex).asString();
            TsFileWriter writer = getOrInitTsFileWriter(record, vsgIndex);
            // register schema
            MeasurementSchema schema;

            schema = new MeasurementSchema(measurement,
                    transferDataType(record.getColumn(valueIndex)), transferEncodeType(record.getColumn(valueIndex)));


            if (!devices.contains(deviceId)) {
                writer.registerTimeseries(new Path(deviceId), schema);
                devices.add(deviceId);
            }


            // construct TsRecord
            TSRecord tsRecord = new TSRecord(time, deviceId);

            DataPoint dPoint;
            try {
                dPoint = transferDataPoint(measurement, record.getColumn(valueIndex));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            tsRecord.addTuple(dPoint);

            // write
            writer.write(tsRecord);

        }

        /**
         * 获取或初始化tsfileWriter实例
         * @param record datax record
         * @param vsgIndex vsgIndex
         * @return TsFileWriter
         */
        private TsFileWriter getOrInitTsFileWriter(Record record, int vsgIndex) throws IOException {

            TsFileWriter writer = writers.get(vsgIndex);
            // 判断tsfile大小是否到达阈值
            if (writer.getIOWriter().getFile().length() > threshold) {
                writer.close();
                writer = null;
            }
            if (writer == null) {
                String tsFileDir = baseDir
                        + File.separator
                        + storeGroup
                        + File.separator
                        + vsgIndex
                        + File.separator
                        + 0;
                FSFactoryProducer.getFSFactory().getFile(tsFileDir).mkdirs();

                String path = record.getColumn(timeIndex).asDate().getTime()
                        + FILE_NAME_SEPARATOR
                        + "0"
                        + FILE_NAME_SEPARATOR
                        + "0"
                        + FILE_NAME_SEPARATOR
                        + "0"
                        + TsFileConstant.TSFILE_SUFFIX;
                File f = FSFactoryProducer.getFSFactory().getFile(tsFileDir+ File.separator+ path);
                try {
                    writer = new TsFileWriter(f);
                    writers.putIfAbsent(vsgIndex, writer);
                    LOG.info("init tsfile writer path: {}", path);
                } catch (IOException e) {
                    LOG.error("get or init tsfile writer error ", e);
                    throw new RuntimeException(e);
                }
            }



            return writer;
        }

        /**
         * 转换成IoTDB数据结构
         * @param measurement measurement
         * @param column column
         * @return DataPoint
         * @throws Exception exception
         */
        private DataPoint transferDataPoint(String measurement, Column column) throws Exception {

            switch (column.getType()) {
                case INT:
                    return new IntDataPoint(measurement, column.asBigInteger().intValue());
                case LONG:
                    return new LongDataPoint(measurement, column.asLong());
                case DOUBLE:
                    return new DoubleDataPoint(measurement, column.asDouble());
                case BOOL:
                    return new BooleanDataPoint(measurement, column.asBoolean());
                case DATE:
                    return new LongDataPoint(measurement, column.asDate().getTime());
                case STRING:
                case BYTES:
                    return new StringDataPoint(measurement, new Binary(column.asString()));
                default:
                    throw new Exception("transfer data point error! ");
            }
        }

        /**
         * 转换成IoTDB类型结构
         * @param column column
         * @return TSDataType
         * @throws Exception exception
         */
        private TSDataType transferDataType(Column column) throws Exception {
            switch (column.getType()) {
                case INT:
                    return TSDataType.INT32;
                case LONG:
                case DATE:
                    return TSDataType.INT64;
                case BOOL:
                    return TSDataType.BOOLEAN;
                case STRING:
                case BYTES:
                    return TSDataType.TEXT;
                case DOUBLE:
                    return TSDataType.DOUBLE;
                default:
                    throw new Exception("transfer data point error! ");
            }
        }

        /**
         * 根据类型使用默认编码方式
         * @param column column
         * @return TSEncoding TSEncoding
         * @throws Exception exception
         */
        private TSEncoding transferEncodeType(Column column) throws Exception {
            switch (column.getType()) {
                case INT:
                case LONG:
                case DATE:
                case DOUBLE:
                    return TSEncoding.GORILLA;
                case BOOL:
                    return TSEncoding.RLE;
                case STRING:
                case BYTES:
                    return TSEncoding.PLAIN;
                default:
                    throw new Exception("transfer data point error! ");
            }
        }

        /**
         * [_time值,_value值,_field值,_measurement值,Tag1Key,Tag1Value,
         * Tag2Key, Tag2Value, ......,TagNKey, TagNValue]
         * 从第一个Tag的value开始
         */
        private String getDeviceFullPath(Record record) {
            // added sg
            StringBuilder sb = new StringBuilder(storeGroup);

            // added _measurement
            sb.append(separator).append(record.getColumn(measurementIndex).asString());

            if (tagStartIndex <= record.getColumnNumber()) {
                // added tags
                for (int i = tagStartIndex; i < record.getColumnNumber(); i = i + 2) {
                    sb.append(separator).append(record.getColumn(i).asString());
                }
            }
            return sb.toString();
        }

    }
}
