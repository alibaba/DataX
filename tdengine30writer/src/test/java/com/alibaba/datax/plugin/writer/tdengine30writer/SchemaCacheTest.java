package com.alibaba.datax.plugin.writer.tdengine30writer;

import com.alibaba.datax.common.util.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SchemaCacheTest {

    private String config;

    @Test
    @Ignore
    public void testSchemaCache() {
        List<Thread> tList = IntStream.range(0, 10).mapToObj(i -> {
            Thread t = new Thread(() -> {
                Configuration config = Configuration.from(this.config);
                SchemaCache schemaCache = SchemaCache.getInstance(config);

                List<ColumnMeta> col_metas = schemaCache.getColumnMetaList("cnpp_ads_wmct_d", TableType.SUP_TABLE);
                Assert.assertEquals(10, col_metas.size());

            });
            return t;
        }).collect(Collectors.toList());

        tList.forEach(Thread::start);

        tList.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    @Before
    public void before() {
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("ts-4558.json");
        try {
            byte[] bytes = new byte[in.available()];
            in.read(bytes);
            this.config = new String(bytes);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}