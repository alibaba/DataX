package com.alibaba.datax.plugin.writer;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.mock.MockRecord;
import com.alibaba.datax.plugin.writer.neo4jwriter.Neo4jClient;
import org.junit.Test;
import org.neo4j.driver.*;
import org.neo4j.driver.types.Node;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * 由于docker 镜像没有apoc函数，所以此测试只能本地搭建环境复现
 */
public class ApocTest {
    /**
     * neo4j中,Label和关系类型,想动态的写，需要借助于apoc函数
     */
    @Test
    public void test_use_apoc_create_dynamic_label() {
        try (Driver neo4jDriver = GraphDatabase.driver(
                "bolt://localhost:7687",
                AuthTokens.basic("yourUserName", "yourPassword"));
             Session neo4jSession = neo4jDriver.session(SessionConfig.forDatabase("yourDataBase"))) {
            List<String> dynamicLabel = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                dynamicLabel.add("Label" + i);
            }
            //删除原有数据
            //remove test data if exist
            //这种占位符的方式不支持批量动态写,当然可以使用union拼接，但是性能不好
            String query = "match (p:%s) return p";
            String delete = "match (p:%s) delete p";
            for (String label : dynamicLabel) {
                Result result = neo4jSession.run(String.format(query, label));
                if (result.hasNext()) {
                    neo4jSession.run(String.format(delete, label));
                }
            }

            Configuration configuration = Configuration.from(new File("src/test/resources/dynamicLabel.json"));
            Neo4jClient neo4jClient = Neo4jClient.build(configuration, null);

            neo4jClient.init();
            for (int i = 0; i < dynamicLabel.size(); i++) {
                Record record = new MockRecord();
                record.addColumn(new StringColumn(dynamicLabel.get(i)));
                record.addColumn(new StringColumn(String.valueOf(i)));
                neo4jClient.tryWrite(record);
            }
            neo4jClient.destroy();

            //校验脚本的批量写入是否正确
            int cnt = 0;
            for (int i = 0; i < dynamicLabel.size(); i++) {
                String label = dynamicLabel.get(i);
                Result result = neo4jSession.run(String.format(query, label));
                while (result.hasNext()) {
                    org.neo4j.driver.Record record = result.next();
                    Node node = record.get("p").asNode();
                    assertTrue(node.hasLabel(label));
                    assertEquals(node.asMap().get("id"), i + "");
                    cnt++;
                }
            }
            assertEquals(cnt, 100);
        }





    }
}
