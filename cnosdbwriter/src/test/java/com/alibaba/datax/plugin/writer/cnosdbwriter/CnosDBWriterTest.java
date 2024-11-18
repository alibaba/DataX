package com.alibaba.datax.plugin.writer.cnosdbwriter;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.SecretUtil;
import junit.framework.TestCase;

import java.util.HashMap;

public class CnosDBWriterTest extends TestCase {
    public void testInitWithFormatNormal() {
        String confJson = "{" +
                "  \"cnosdbWriteAPI\": \"http://127.0.0.1:8902/api/v1/write\"," +
                "  \"tenant\": \"t1\"," +
                "  \"database\": \"d1\"," +
                "  \"username\": \"u1\"," +
                "  \"password\": \"p1\"," +
                "  \"batchSize\": 1," +
                "  \"format\": \"datax\"," +
                "  \"table\": \"t2\"," +
                "  \"tags\": {" +
                "    \"t3\": 1" +
                "  }," +
                "  \"fields\": {" +
                "    \"f1\": 2" +
                "  }," +
                "  \"timeIndex\": 0," +
                "  \"precision\": \"ms\"," +
                "  \"tagsExtra\": {" +
                "    \"t3\": \"3\"," +
                "    \"t4\": \"4\"," +
                "    \"t5\": \"5\"" +
                "  }" +
                "}";
        Configuration conf = Configuration.from(confJson);
        CnosDBWriter.Task task = new CnosDBWriter.Task();
        task.setPluginJobConf(conf);
        task.init();

        assertEquals(task.tenant, "t1");
        assertEquals(task.database, "d1");
        assertEquals(task.username, "u1");
        assertEquals(task.password, "p1");
        assertEquals(task.batchSize, 1);
        assertEquals(task.format, "datax");
        assertEquals(task.table, "t2");
        assertEquals(task.tagIndexes.get(1), "t3");
        assertEquals(task.fieldIndexes.get(2), "f1");
        assertEquals(task.timeIndex, Integer.valueOf(0));
        assertEquals(task.precision, "ms");
        assert !task.tagsExtra.containsKey("t3");
        assertEquals(task.tagsExtra.get("t4"), "4");
        assertEquals(task.tagsExtra.get("t5"), "5");

        assertEquals(task.precisionMultiplier, 1_000_000);
        assertEquals(task.writeReqUrl, "http://127.0.0.1:8902/api/v1/write?precision=ns&tenant=t1&db=d1");
        assert task.basicAuth.startsWith("Basic ");
        String decryptAuth = new String(SecretUtil.decryptBASE64(task.basicAuth.substring("Basic ".length())));
        assertEquals(decryptAuth, "u1:p1");
    }

    public void testInitWithFormatOpenTSDB() {
        String confJson = "{" +
                "  \"cnosdbWriteAPI\": \"http://127.0.0.1:8902/api/v1/write\"," +
                "  \"tenant\": \"t2\"," +
                "  \"database\": \"d2\"," +
                "  \"username\": \"u2\"," +
                "  \"password\": \"p2\"," +
                "  \"batchSize\": 2," +
                "  \"format\": \"opentsdb\"," +
                "  \"precision\": \"ms\"," +
                "  \"tagsExtra\": {" +
                "    \"t1\": \"1\"," +
                "    \"t2\": \"2\"," +
                "  }," +
                "  \"fieldsExtra\": {" +
                "    \"f1\": {" +
                "      \"table\": \"t3\"," +
                "      \"field\": \"f1\"," +
                "    }," +
                "    \"f2\": {" +
                "      \"table\": \"t4\"," +
                "      \"field\": \"f2\"," +
                "      \"tagsExtra\": {" +
                "        \"t3\": \"3\"," +
                "        \"t4\": \"4\"," +
                "      }" +
                "    }" +
                "  }" +
                "}";
        Configuration conf = Configuration.from(confJson);
        CnosDBWriter.Task task = new CnosDBWriter.Task();
        task.setPluginJobConf(conf);
        task.init();

        assertEquals(task.tenant, "t2");
        assertEquals(task.database, "d2");
        assertEquals(task.username, "u2");
        assertEquals(task.password, "p2");
        assertEquals(task.batchSize, 2);
        assertEquals(task.format, "opentsdb");
        assertNull(task.table);
        assertNull(task.tagIndexes);
        assertNull(task.fieldIndexes);
        assertNull(task.timeIndex);
        assertEquals(task.precision, "ms");
        assertEquals(task.tagsExtra.get("t1"), "1");
        assertEquals(task.tagsExtra.get("t2"), "2");
        assertEquals(
                task.fieldsExtra.get("f1"),
                new CnosDBWriterConfigFieldExtra("t3", "f1", new HashMap<String, String>() {{
                    put("t1", "1");
                    put("t2", "2");
                }}));
        assertEquals(
                task.fieldsExtra.get("f2"),
                new CnosDBWriterConfigFieldExtra("t4", "f2", new HashMap<String, String>() {{
                    put("t3", "3");
                    put("t4", "4");
                }}));

        assertEquals(task.precisionMultiplier, 1_000_000);
        assertEquals(task.writeReqUrl, "http://127.0.0.1:8902/api/v1/write?precision=ns&tenant=t2&db=d2");
        assert task.basicAuth.startsWith("Basic ");
        String decryptAuth = new String(SecretUtil.decryptBASE64(task.basicAuth.substring("Basic ".length())));
        assertEquals(decryptAuth, "u2:p2");
    }
}
