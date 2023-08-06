package com.alibaba.datax.plugin.writer.mock;

import com.alibaba.datax.example.ExampleContainer;
import com.alibaba.datax.example.util.PathUtil;
import com.alibaba.datax.plugin.writer.Neo4jWriterTest;
import org.junit.Assert;
import org.junit.Test;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;

/**
 * 展示如何使用ExampleContainer运行测试用例
 * {@code Author} FuYouJ
 * {@code Date} 2023/8/6 11:36
 */

public class StreamReader2Neo4jWriterTest extends Neo4jWriterTest {
    private static final int CHANNEL = 5;
    private static final int READER_NUM = 10;

    //在neo4jWriter模块使用Example测试整个job,方便发现整个流程的代码问题
    @Test
    public void streamReader2Neo4j() {

        deleteHistoryIfExist();

        String path = "/streamreader2neo4j.json";
        String jobPath = PathUtil.getAbsolutePathFromClassPath(path);

        ExampleContainer.start(jobPath);

        //根据channel和reader的mock数据，校验结果集是否符合预期
        verifyWriteResult();
    }

    private void deleteHistoryIfExist() {
        String query = "match (n:StreamReader) return n limit 1";
        String delete = "match (n:StreamReader) delete n";
        if (super.neo4jSession.run(query).hasNext()) {
            neo4jSession.run(delete);
        }
    }

    private void verifyWriteResult() {
        int total = CHANNEL * READER_NUM;
        String query = "match (n:StreamReader) return n";
        Result run = neo4jSession.run(query);
        int count = 0;
        while (run.hasNext()) {
            Record record = run.next();
            Node node = record.get("n").asNode();
            if (node.hasLabel("StreamReader")) {
                count++;
            }
        }
        Assert.assertEquals(count, total);
    }
}
