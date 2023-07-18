package com.alibaba.datax.plugin.reader.clickhousereader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.dataxservice.face.eventcenter.EventLogStore;
import com.alibaba.datax.dataxservice.face.eventcenter.RuntimeContext;
import com.alibaba.datax.test.simulator.BasicReaderPluginTest;
import com.alibaba.datax.test.simulator.junit.extend.log.LoggedRunner;
import com.alibaba.datax.test.simulator.junit.extend.log.TestLogger;
import com.alibaba.fastjson.JSON;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;


@RunWith(LoggedRunner.class)
@Ignore
public class ClickhouseReaderTest extends BasicReaderPluginTest {
    @TestLogger(log = "测试basic1.json. 配置常量.")
    @Test
    public void testBasic1() {
        RuntimeContext.setGlobalJobId(-1);
        EventLogStore.init();
        List<Record> noteRecordForTest = new ArrayList<Record>();

        List<Configuration> subjobs = super.doReaderTest("basic1.json", 1, noteRecordForTest);

        Assert.assertEquals(1, subjobs.size());
        Assert.assertEquals(1, noteRecordForTest.size());

        Assert.assertEquals("[8,16,32,64,-8,-16,-32,-64,\"3.2\",\"6.4\",1,\"str_col\",\"abc\"," + "\"417ddc5d-e556-4d27-95dd-a34d84e46a50\",1580745600000,1580752800000,\"hello\",\"[1,2,3]\"," + "\"[\\\"abc\\\",\\\"cde\\\"]\",\"(8,'uint8_type')\",null,\"[1,2]\",\"[\\\"x\\\",\\\"y\\\"]\",\"127.0.0.1\",\"::\",\"23.345\"]", JSON.toJSONString(listData(noteRecordForTest.get(0))));
    }

    @Override
    protected OutputStream buildDataOutput(String optionalOutputName) {
        File f = new File(optionalOutputName + "-output.txt");
        try {
            return new FileOutputStream(f);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public String getTestPluginName() {
        return "clickhousereader";
    }

    private Object[] listData(Record record) {
        if (null == record) {
            return ArrayUtils.EMPTY_OBJECT_ARRAY;
        }
        Object[] arr = new Object[record.getColumnNumber()];
        for (int i = 0; i < arr.length; i++) {
            Column col = record.getColumn(i);
            if (null != col) {
                arr[i] = col.getRawData();
            }
        }
        return arr;
    }
}
