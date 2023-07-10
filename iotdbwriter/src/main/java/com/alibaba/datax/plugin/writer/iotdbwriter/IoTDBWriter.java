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

      // Create all Databases if not exist
      String[] databases = JOB_CONF.getMap(IoTDBWriterConfig.DATABASES).keySet().toArray(new String[0]);
      if (databases.length == 0) {
        throw DataXException.asDataXException(
          IoTDBWriterErrorCode.CONF_ERROR, "databases must be specified");
      }
      SessionPool sessionPool = generateSessionPool(JOB_CONF);
      for (String database : databases) {
        try {
          sessionPool.createDatabase(database);
          LOGGER.info("[IoTDBWriter.Job.init] create database: {}", database);
        } catch (Exception e) {
          // Ignore
        }
      }
      sessionPool.close();
    }

    @Override
    public List<Configuration> split(int mandatoryNumber) {
      List<Configuration> splitConfigs = new ArrayList<>();
      for (int i = 0; i < mandatoryNumber; i++) {
        // Initialize basic configurations
        Configuration conf = Configuration.newDefault();
        conf.set(IoTDBWriterConfig.ADDRESSES, JOB_CONF.get(IoTDBWriterConfig.ADDRESSES));
        conf.set(IoTDBWriterConfig.USERNAME, JOB_CONF.get(IoTDBWriterConfig.USERNAME));
        conf.set(IoTDBWriterConfig.PASSWORD, JOB_CONF.get(IoTDBWriterConfig.PASSWORD));
        conf.set(IoTDBWriterConfig.SESSION_POOL_MAX_SIZE, JOB_CONF.get(IoTDBWriterConfig.SESSION_POOL_MAX_SIZE));
        conf.set(IoTDBWriterConfig.BATCH_SIZE, JOB_CONF.get(IoTDBWriterConfig.BATCH_SIZE));
        splitConfigs.add(conf);
      }

      // Split devices evenly
      int splitCounter = 0;
      Map<Integer, Map<String, Map<String, Map<String, List<String>>>>> splitMap = new HashMap<>();
      Map<String, Object> rawDatabaseMap = JOB_CONF.getMap(IoTDBWriterConfig.DATABASES);
      for (Map.Entry<String, Object> databaseEntry : rawDatabaseMap.entrySet()) {
        String database = databaseEntry.getKey();
        Map<String, Map<String, List<String>>> rawDeviceMap =
          (Map<String, Map<String, List<String>>>) databaseEntry.getValue();
        for (Map.Entry<String, Map<String, List<String>>> deviceEntry : rawDeviceMap.entrySet()) {
          splitMap
            .computeIfAbsent(splitCounter, k -> new HashMap<>())
            .computeIfAbsent(database, k -> new HashMap<>())
            .put(deviceEntry.getKey(), deviceEntry.getValue());
          splitCounter = (splitCounter + 1) % mandatoryNumber;
        }
      }
      splitMap.forEach((k, v) -> {
        if (v.size() > 1 || v.values().iterator().next().size() > 1) {
          throw DataXException.asDataXException(
            IoTDBWriterErrorCode.CONF_ERROR,
            "The channel number must be equal to the sum of devices");
        }
        LOGGER.info("[IoTDBReader.Job.split] split: {} configuration: {}", k, v);
        splitConfigs.get(k).set(IoTDBWriterConfig.DATABASES, v);
      });
      return splitConfigs;
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

    private String deviceId;
    private List<MeasurementSchema> schemas;

    @Override
    public void init() {
      Configuration taskConf = getPluginJobConf();
      LOGGER.info("[IoTDBWriter.Task.init] task configuration: {}", taskConf.toJSON());
      sessionPool = generateSessionPool(taskConf);
      batchSize = taskConf.getInt(IoTDBWriterConfig.BATCH_SIZE, IoTDBWriterConfig.DEFAULT_BATCH_SIZE);

      // Analyze schemas
      Map<String, Object> rawDatabaseMap = taskConf.getMap(IoTDBWriterConfig.DATABASES);
      rawDatabaseMap.forEach((database, rawDeviceMap) -> {
        Map<String, Map<String, List<String>>>
          deviceSchemaMap = (Map<String, Map<String, List<String>>>) rawDeviceMap;
        deviceSchemaMap.forEach((deviceId, schema) -> {
          List<String> sensors = schema.get(IoTDBWriterConfig.SENSORS);
          List<String> dataTypes = schema.get(IoTDBWriterConfig.DATA_TYPES);
          this.deviceId = database + "." + deviceId;
          this.schemas = new ArrayList<>();

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

          // Store MeasurementSchema for current Device
          for (int i = 0; i < sensors.size(); i++) {
            String sensor = sensors.get(i);
            String dataType = dataTypes.get(i);
            this.schemas.add(new MeasurementSchema(sensor, TSDataType.valueOf(dataType)));
          }
        });
      });

      LOGGER.info("[IoTDBWriter.Task.init] device: {} schemas: {}", deviceId, schemas);
    }

    @Override
    public void startWrite(RecordReceiver lineReceiver) {
      LOGGER.info("[IoTDBWriter.Task.startWrite] start to write data");

      Record record;
      int recordCount = 0;
      Tablet tablet = new Tablet(deviceId, schemas, batchSize);
      while ((record = lineReceiver.getFromReader()) != null) {
        // Read timestamp by default(column 0)
        long timestamp = record.getColumn(0).asLong();
        int columnNumber = record.getColumnNumber();
        if (timestamp <= 0) {
          throw DataXException.asDataXException(
            IoTDBWriterErrorCode.WRITE_ERROR,
            String.format("timestamp: %d must be positive", timestamp));
        }

        // Ensure column number matched
        if (schemas.size() != columnNumber - 1) {
          throw DataXException.asDataXException(
            IoTDBWriterErrorCode.WRITE_ERROR,
            String.format("columns of device: %s must be %d", deviceId, schemas.size() + 1));
        }

        // Maintain Tablet
        tablet.addTimestamp(recordCount, timestamp);
        for (int i = 1; i < columnNumber; i++) {
          Column column = record.getColumn(i);
          MeasurementSchema measurementSchema = schemas.get(i - 1);
          String measurementId = measurementSchema.getMeasurementId();
          TSDataType dataType = measurementSchema.getType();
          switch (dataType) {
            case INT32:
              tablet.addValue(measurementId, recordCount, column.asLong().intValue());
              break;
            case INT64:
              tablet.addValue(measurementId, recordCount, column.asLong());
              break;
            case FLOAT:
              tablet.addValue(measurementId, recordCount, column.asDouble().floatValue());
              break;
            case DOUBLE:
              tablet.addValue(measurementId, recordCount, column.asDouble());
              break;
            case BOOLEAN:
              tablet.addValue(measurementId, recordCount, column.asBoolean());
              break;
            case TEXT:
              tablet.addValue(measurementId, recordCount, column.asString());
              break;
            default:
              throw DataXException.asDataXException(
                IoTDBWriterErrorCode.WRITE_ERROR,
                String.format("unsupported data type: %s", dataType));
          }
        }

        recordCount++;
        if (recordCount % batchSize == 0) {
          try {
            tablet.rowSize = recordCount;
            sessionPool.insertTablet(tablet);
            LOGGER.info("[IoTDBWriter.Task.startWrite] write {} records, {} rowNum, {} occupation", recordCount, tablet.getMaxRowNumber(), tablet.getTotalValueOccupation());
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
        try {
          tablet.rowSize = recordCount;
          sessionPool.insertTablet(tablet);
          LOGGER.info("[IoTDBWriter.Task.startWrite] write {} records, {} rowNum, {} occupation", recordCount, tablet.getMaxRowNumber(), tablet.getTotalValueOccupation());
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
