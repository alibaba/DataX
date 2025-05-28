package com.alibaba.datax.example.iotdb;

import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.session.Session;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

public class TestCreateData {
    private static Session session;
    private static Random random = new Random();

    @Test
    public void createAndInsert()
            throws IoTDBConnectionException, StatementExecutionException {
        // 创建测试数据
        // session init
        session =
                new Session.Builder()
                        // .host("192.168.150.100")
                        .host("172.20.31.61")
                        .port(6667)
                        .username("root")
                        .password("root")
                        .version(Version.V_0_13)
                        .build();

        // open session, close RPCCompression
        session.open(false);

        // set session fetchSize
        session.setFetchSize(10000);

        // 创建测点并插入数据
        String filePath = "src/test/resources/testData.txt";
        String database = "root.cgn";
        try {
            session.createDatabase(database);
        } catch (StatementExecutionException e) {
            if (e.getStatusCode() != TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()) {
                throw e;
            }
        }
        String device = "root.cgn.device";
        createAndInsert2(filePath, device);
    }

    private static void createAndInsert2(String filePath, String device)
            throws IoTDBConnectionException, StatementExecutionException {
        // 读取文件(文件中无表头)
        // 点的类型        点名        描述        量纲        量程下限        量程上限
        // AX        L2KRT008MA        测试性描述        %        0        1.00E+02
        // AX        L2ETY101MP        测试性描述        %        0        1.00E+02
        List<List<String>> res = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] words = line.split("\\s+");
                List<String> wordList = new ArrayList<>(Arrays.asList(words));
                res.add(wordList);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 准备传入的参数，构造时间序列
        List<String> paths = new ArrayList<>();
        List<String> measurements = new ArrayList<>();
        List<Boolean> isDoubleList = new ArrayList<>();
        List<TSDataType> tsDataTypes = new ArrayList<>();
        List<TSEncoding> tsEncodings = new ArrayList<>();
        List<CompressionType> compressionTypes = new ArrayList<>();
        List<Map<String, String>> tagsList = new ArrayList<>();
        List<Map<String, String>> attributesList = new ArrayList<>();
        List<String> alias = new ArrayList<>();

        for (int i = 0; i < res.size(); i++) {
            measurements.add(res.get(i).get(1));
            paths.add(device + "." + res.get(i).get(1));
            boolean isDouble = "AX".equals(res.get(i).get(0));
            isDoubleList.add(isDouble);
            tsDataTypes.add(isDouble ? TSDataType.DOUBLE : TSDataType.BOOLEAN);
            tsEncodings.add(isDouble ? TSEncoding.GORILLA : TSEncoding.RLE);
            compressionTypes.add(CompressionType.SNAPPY);
            Map<String, String> attributes = new HashMap<>();
            attributes.put("描述", "测试性描述");
            attributes.put("量纲", isDouble ? "%" : "");
            attributes.put("量程下限", "0");
            attributes.put("量程上限", isDouble ? "1.00E+02" : "2.00E+02");
            attributesList.add(attributes);
        }

        // 先删除已有的时间序列
        if (session.checkTimeseriesExists(device + ".**")) {
            session.deleteTimeseries(device + ".**");
            System.out.println("删除已有的时间序列完成==============");
        }

        // 创建测点时间序列
        session.createMultiTimeseries(
                paths, tsDataTypes, tsEncodings, compressionTypes, null, null, attributesList, null);

        // 插入数据：每个测点里都写1万条数据，时间间隔1秒
        List<List<String>> measurementsList = new ArrayList<>();
        List<List<Object>> valuesList = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();
        List<List<TSDataType>> typesList = new ArrayList<>();

        long startTime =
                LocalDateTime.of(2024, 1, 1, 0, 0, 0, 0).toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
        int count = 10000; // 每个测点插入数据条数

        for (long time = startTime; count >= 0; time += 1000, count--) {
            timestamps.add(time);
            measurementsList.add(measurements); // 39个测点
            typesList.add(tsDataTypes);

            List<Object> randomValue = new ArrayList<>();
            for (Boolean isDouble : isDoubleList) {
                randomValue.add(isDouble ? random.nextDouble() * 100 : random.nextBoolean());
            }
            valuesList.add(randomValue); // 39个随机数

            // 每1000次插入一批数据
            if (count != 10000 && count % 1000 == 0) {
                session.insertRecordsOfOneDevice(
                        device, timestamps, measurementsList, typesList, valuesList);
                measurementsList.clear();
                valuesList.clear();
                typesList.clear();
                timestamps.clear();
                valuesList.clear();
            }
        }
    }
}
