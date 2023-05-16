package com.alibaba.datax.plugin.writer.iotdbwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IoTDBWriter extends Writer {

  public static class Job extends Writer.Job {

    private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBWriter.Job.class);

    private Configuration JOB_CONF;

    @Override
    public void init() {
      JOB_CONF = getPluginJobConf();
      LOGGER.info("[IoTDBWriter.Job.init] job configuration: {}", JOB_CONF.toJSON());

      // Create all Databases if not exist
      List<String> databases = JOB_CONF.getList(IoTDBWriterConfig.DATABASES, String.class);
      if (databases == null || databases.isEmpty()) {
        throw DataXException.asDataXException(
          IoTDBWriterErrorCode.CONF_ERROR, "databases must be specified");
      }
      SessionPool sessionPool = generateSessionPool(JOB_CONF);
      for (String database : databases) {
        try {
          sessionPool.createDatabase(database);
          LOGGER.info("[IoTDBWriter.Job.init] create database: {}", database);
        } catch (Exception e) {
          throw DataXException.asDataXException(
            IoTDBWriterErrorCode.WRITE_ERROR, e);
        }
      }
      sessionPool.close();
    }

    @Override
    public List<Configuration> split(int mandatoryNumber) {
      List<Configuration> splitResultConfigs = new ArrayList<>();
      for (int j = 0; j < mandatoryNumber; j++) {
        splitResultConfigs.add(JOB_CONF.clone());
      }
      return splitResultConfigs;
    }

    @Override
    public void destroy() {
      // Do nothing
    }
  }

  public static class Task extends Writer.Task {

    private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBWriter.Task.class);

    private SessionPool sessionPool;
    private int batchSize;

    // Map<DeviceId, List<MeasurementSchema>>
    private final Map<String, List<MeasurementSchema>> iotdbSchemaMap = new HashMap<>();

    @Override
    public void init() {
      Configuration taskConf = getPluginJobConf();
      LOGGER.info("[IoTDBWriter.Task.init] task configuration: {}", taskConf.toJSON());
      sessionPool = generateSessionPool(taskConf);
      batchSize = taskConf.getInt(IoTDBWriterConfig.BATCH_SIZE, IoTDBWriterConfig.DEFAULT_BATCH_SIZE);

      // Analyze all schemas
      Map<String, Object> rawDatabaseMap = taskConf.getMap(IoTDBWriterConfig.DATABASES);
      rawDatabaseMap.forEach((database, rawDeviceMap) -> {
        Map<String, Map<String, List<String>>>
          deviceSchemaMap = (Map<String, Map<String, List<String>>>) rawDeviceMap;
        deviceSchemaMap.forEach((deviceId, schema) -> {
          List<String> sensors = schema.get(IoTDBWriterConfig.SENSORS);
          List<String> dataTypes = schema.get(IoTDBWriterConfig.DATA_TYPES);
          if (sensors == null || sensors.isEmpty()) {
            throw DataXException.asDataXException(
              IoTDBWriterErrorCode.CONF_ERROR,
              String.format("sensors of device: %s must be specified", deviceId));
          }
          if (dataTypes == null || dataTypes.isEmpty()) {
            throw DataXException.asDataXException(
              IoTDBWriterErrorCode.CONF_ERROR,
              String.format("dataTypes of device: %s must be specified", deviceId));
          }
          if (sensors.size() != dataTypes.size()) {
            throw DataXException.asDataXException(
              IoTDBWriterErrorCode.CONF_ERROR,
              String.format("sensors and dataTypes of device: %s must be the same size", deviceId));
          }

          // Store MeasurementSchema for each Device
          List<MeasurementSchema> iotdbSchema = new ArrayList<>();
          for (int i = IoTDBWriterConfig.MEASUREMENT_OFFSET; i < sensors.size(); i++) {
            String sensor = sensors.get(i);
            String dataType = dataTypes.get(i);
            iotdbSchema.add(new MeasurementSchema(sensor, TSDataType.valueOf(dataType)));
          }
          iotdbSchemaMap.put(deviceId, iotdbSchema);
        });
      });
    }

    @Override
    public void startWrite(RecordReceiver lineReceiver) {
      LOGGER.info("[IoTDBWriter.Task.startWrite] start to write data");

      Record record;
      int recordCount = 0;
      Map<String, Tablet> tablets = new HashMap<>();
      while ((record = lineReceiver.getFromReader()) != null) {
        // Read Device and Timestamp by default
        String deviceId = record.getColumn(0).asString();
        long timestamp = record.getColumn(1).asLong();
        int columnNumber = record.getColumnNumber();
        if (!iotdbSchemaMap.containsKey(deviceId)) {
          throw DataXException.asDataXException(
            IoTDBWriterErrorCode.WRITE_ERROR,
            String.format("device: %s doesn't exist", deviceId));
        }
        if (timestamp <= 0) {
          throw DataXException.asDataXException(
            IoTDBWriterErrorCode.WRITE_ERROR,
            String.format("timestamp: %d must be positive", timestamp));
        }

        // Ensure column number matched
        List<MeasurementSchema> iotdbSchema = iotdbSchemaMap.get(deviceId);
        if (iotdbSchema.size() != columnNumber - IoTDBWriterConfig.MEASUREMENT_OFFSET) {
          throw DataXException.asDataXException(
            IoTDBWriterErrorCode.WRITE_ERROR,
            String.format("columns of device: %s must be %d", deviceId, iotdbSchema.size() + IoTDBWriterConfig.MEASUREMENT_OFFSET));
        }

        // Maintain Tablet
        tablets
          .computeIfAbsent(deviceId, empty -> new Tablet(deviceId, iotdbSchemaMap.get(deviceId), batchSize))
          .addTimestamp(recordCount, timestamp);
        for (int i = IoTDBWriterConfig.MEASUREMENT_OFFSET; i < columnNumber; i++) {
          Column column = record.getColumn(i);
          MeasurementSchema measurementSchema = iotdbSchema.get(i - IoTDBWriterConfig.MEASUREMENT_OFFSET);
          String measurementId = measurementSchema.getMeasurementId();
          TSDataType dataType = measurementSchema.getType();
          switch (dataType) {
            case INT32:
              tablets.get(deviceId).addValue(measurementId, recordCount, column.asLong().intValue());
              break;
            case INT64:
              tablets.get(deviceId).addValue(measurementId, recordCount, column.asLong());
              break;
            case FLOAT:
              tablets.get(deviceId).addValue(measurementId, recordCount, column.asDouble().floatValue());
              break;
            case DOUBLE:
              tablets.get(deviceId).addValue(measurementId, recordCount, column.asDouble());
              break;
            case BOOLEAN:
              tablets.get(deviceId).addValue(measurementId, recordCount, column.asBoolean());
              break;
            case TEXT:
              tablets.get(deviceId).addValue(measurementId, recordCount, column.asString());
              break;
            default:
              throw DataXException.asDataXException(
                IoTDBWriterErrorCode.WRITE_ERROR,
                String.format("unsupported data type: %s", dataType));
          }
        }

        recordCount++;
        if (recordCount % batchSize == 0) {
          LOGGER.info("[IoTDBWriter.Task.startWrite] write {} records", recordCount);
          try {
            sessionPool.insertTablets(tablets);
          } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw DataXException.asDataXException(
              IoTDBWriterErrorCode.WRITE_ERROR,
              String.format("failed to write data, error: %s", e.getMessage()));
          } finally {
            recordCount = 0;
          }
        }
      }

      if (recordCount > 0) {
        LOGGER.info("[IoTDBWriter.Task.startWrite] write {} records", recordCount);
        try {
          sessionPool.insertTablets(tablets);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
          throw DataXException.asDataXException(
            IoTDBWriterErrorCode.WRITE_ERROR,
            String.format("failed to write data, error: %s", e.getMessage()));
        }
      }
    }

    @Override
    public void destroy() {
      sessionPool.close();
    }
  }

  private static SessionPool generateSessionPool(Configuration conf) {
    SessionPool sessionPool;
    String rawNodeUrls = conf.getString(IoTDBWriterConfig.ADDRESSES);
    List<String> nodeUrls = Arrays.asList(rawNodeUrls.split(","));
    sessionPool =
      new SessionPool.Builder()
        .nodeUrls(nodeUrls)
        .user(conf.getString(IoTDBWriterConfig.USERNAME))
        .password(conf.getString(IoTDBWriterConfig.PASSWORD))
        .maxSize(conf.getInt(IoTDBWriterConfig.SESSION_POOL_MAX_SIZE, IoTDBWriterConfig.DEFAULT_SESSION_POOL_MAX_SIZE))
        .build();
    return sessionPool;
  }
}
