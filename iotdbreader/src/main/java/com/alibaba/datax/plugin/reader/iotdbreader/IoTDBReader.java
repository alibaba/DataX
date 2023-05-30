package com.alibaba.datax.plugin.reader.iotdbreader;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IoTDBReader extends Reader {

  public static class Job extends Reader.Job {

    private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBReader.Job.class);

    @Override
    public void init() {
      // Do nothing
    }

    @Override
    public void destroy() {
      // Do nothing
    }

    @Override
    public List<Configuration> split(int adviceNumber) {

      Configuration jobConf = super.getPluginJobConf();
      List<Configuration> splitConfigs = new ArrayList<>();
      for (int i = 0; i < adviceNumber; i++) {
        // Initialize basic configurations
        Configuration conf = Configuration.newDefault();
        conf.set(IoTDBReaderConfig.ADDRESSES, jobConf.get(IoTDBReaderConfig.ADDRESSES));
        conf.set(IoTDBReaderConfig.USERNAME, jobConf.get(IoTDBReaderConfig.USERNAME));
        conf.set(IoTDBReaderConfig.PASSWORD, jobConf.get(IoTDBReaderConfig.PASSWORD));
        splitConfigs.add(conf);
      }

      // Split devices evenly
      int splitCounter = 0;
      Map<Integer, Map<String, Map<String, Map<String, List<String>>>>> splitMap = new HashMap<>();
      Map<String, Object> rawDatabaseMap = jobConf.getMap(IoTDBReaderConfig.DATABASES);
      for (Map.Entry<String, Object> databaseEntry : rawDatabaseMap.entrySet()) {
        String database = databaseEntry.getKey();
        Map<String, Map<String, List<String>>> rawDeviceMap =
          (Map<String, Map<String, List<String>>>) databaseEntry.getValue();
        for (Map.Entry<String, Map<String, List<String>>> deviceEntry : rawDeviceMap.entrySet()) {
          splitMap
            .computeIfAbsent(splitCounter, k -> new HashMap<>())
            .computeIfAbsent(database, k -> new HashMap<>())
            .put(deviceEntry.getKey(), deviceEntry.getValue());
          splitCounter = (splitCounter + 1) % adviceNumber;
        }
      }
      splitMap.forEach((k, v) -> {
        if (v.size() > 1 || v.values().iterator().next().size() > 1) {
          throw DataXException.asDataXException(
            IoTDBReaderErrorCode.CONF_ERROR,
            "The channel number must be equal to the sum of devices");
        }
        LOGGER.info("[IoTDBReader.Job.split] split: {} configuration: {}", k, v);
        splitConfigs.get(k).set(IoTDBReaderConfig.DATABASES, v);
      });
      return splitConfigs;
    }
  }

  public static class Task extends Reader.Task {

    private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBReader.Task.class);

    private SessionPool sessionPool;

    @Override
    public void init() {
      LOGGER.info("[IoTDBReader.Task.init] task configuration: {}", super.getPluginJobConf().toJSON());
      Configuration taskConf = super.getPluginJobConf();
      String rawNodeUrls = taskConf.getString(IoTDBReaderConfig.ADDRESSES);
      List<String> nodeUrls = Arrays.asList(rawNodeUrls.split(","));
      sessionPool =
        new SessionPool.Builder()
          .nodeUrls(nodeUrls)
          .user(taskConf.getString(IoTDBReaderConfig.USERNAME))
          .password(taskConf.getString(IoTDBReaderConfig.PASSWORD))
          .maxSize(taskConf.getInt(IoTDBReaderConfig.SESSION_POOL_MAX_SIZE, IoTDBReaderConfig.DEFAULT_SESSION_POOL_MAX_SIZE))
          .build();
      LOGGER.info("[IoTDBReader.Task.init] SessionPool initialized");
    }

    @Override
    public void destroy() {
      sessionPool.close();
    }

    @Override
    public void startRead(RecordSender recordSender) {
      Configuration taskConf = super.getPluginJobConf();
      Map<String, Object> rawDatabaseMap = taskConf.getMap(IoTDBReaderConfig.DATABASES);
      rawDatabaseMap.forEach((database, rawDeviceMap) -> {
        Map<String, Map<String, List<String>>> deviceMap = (Map<String, Map<String, List<String>>>) rawDeviceMap;
        deviceMap.forEach((device, schema) -> {
          List<String> sensors = schema.get(IoTDBReaderConfig.SENSORS);
          List<String> dataTypes = schema.get(IoTDBReaderConfig.DATA_TYPES);
          String sql = String.format("select * from %s align by device", database + "." + device);
          LOGGER.info("[IoTDBReader.Task.startRead] database: {} device: {} sensors: {} dataTypes: {}",
            database, device, sensors, dataTypes);

          try (SessionDataSetWrapper dataSetWrapper = sessionPool.executeQueryStatement(sql)) {
            List<String> columnNames = dataSetWrapper.getColumnNames();
            List<String> columnTypes = dataSetWrapper.getColumnTypes();
            LOGGER.info("[IoTDBReader.Task.startRead] columnNames: {} columnTypes: {}", columnNames, columnTypes);

            List<Integer> schemaMatchList = new ArrayList<>();
            for (int i = 0; i < sensors.size(); i++) {
              String sensor = sensors.get(i);
              String dataType = dataTypes.get(i);
              boolean matched = false;
              // j starts from 1 to skip timestamp column
              for (int j = 1; j < columnNames.size(); j++) {
                String columnName = columnNames.get(j);
                String columnType = columnTypes.get(j);
                if (sensor.equals(columnName) && dataType.equals(columnType)) {
                  schemaMatchList.add(j - 1);
                  matched = true;
                  break;
                }
              }
              if (!matched) {
                LOGGER.error("[IoTDBReader.Task.startRead] sensor: {} dataType: {} not found in IoTDB, please check your json file.", sensor, dataType);
                return;
              }
            }

            while (dataSetWrapper.hasNext()) {
              RowRecord rowRecord = dataSetWrapper.next();
              Record record = recordSender.createRecord();
              // Always add timestamp first
              record.addColumn(new LongColumn(rowRecord.getTimestamp()));
              convertToDataXRecord(rowRecord, record, columnTypes, schemaMatchList);
              recordSender.sendToWriter(record);
            }
          } catch (StatementExecutionException | IoTDBConnectionException e) {
            LOGGER.error("[IoTDBReader.Task.startRead] error: {}", e.getMessage());
          }
        });
      });
    }

    private void convertToDataXRecord(RowRecord rowRecord, Record record, List<String> columnTypes, List<Integer> schemaMatchList) {
      List<Field> fields = rowRecord.getFields();
      for (int index : schemaMatchList) {
        Field field = fields.get(index);
        String dataType = columnTypes.get(index);
        switch (dataType) {
          case "BOOLEAN":
            record.addColumn(new BoolColumn(field.getBoolV()));
            break;
          case "INT32":
            record.addColumn(new LongColumn(field.getIntV()));
            break;
          case "INT64":
            record.addColumn(new LongColumn(field.getLongV()));
            break;
          case "FLOAT":
            record.addColumn(new DoubleColumn(field.getFloatV()));
            break;
          case "DOUBLE":
            record.addColumn(new DoubleColumn(field.getDoubleV()));
            break;
          case "TEXT":
            record.addColumn(new StringColumn(field.getStringValue()));
            break;
          default:
            LOGGER.error("[IoTDBReader.Task.convertToDataXRecord] unsupported data type: {}", dataType);
            break;
        }
      }
    }
  }
}
