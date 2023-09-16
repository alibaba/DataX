package com.alibaba.datax.plugin.writer;


import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.mock.MockRecord;
import com.alibaba.datax.plugin.writer.mock.MockUtil;
import com.alibaba.datax.plugin.writer.neo4jwriter.Neo4jClient;
import com.alibaba.datax.plugin.writer.neo4jwriter.config.Neo4jProperty;
import com.alibaba.datax.plugin.writer.neo4jwriter.element.PropertyType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.driver.*;
import org.neo4j.driver.types.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class Neo4jWriterTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jWriterTest.class);
    private static final int MOCK_NUM = 100;
    private static final String CONTAINER_IMAGE = "neo4j:5.9.0";

    private static final String CONTAINER_HOST = "neo4j-host";
    private static final int HTTP_PORT = 7474;
    private static final int BOLT_PORT = 7687;
    private static final String CONTAINER_NEO4J_USERNAME = "neo4j";
    private static final String CONTAINER_NEO4J_PASSWORD = "Test@12343";
    private static final URI CONTAINER_URI = URI.create("neo4j://localhost:" + BOLT_PORT);

    protected static final Network NETWORK = Network.newNetwork();

    private GenericContainer<?> container;
    private Driver neo4jDriver;
    private Session neo4jSession;

    @Before
    public void init() {
        DockerImageName imageName = DockerImageName.parse(CONTAINER_IMAGE);
        container =
                new GenericContainer<>(imageName)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(CONTAINER_HOST)
                        .withExposedPorts(HTTP_PORT, BOLT_PORT)
                        .withEnv(
                                "NEO4J_AUTH",
                                CONTAINER_NEO4J_USERNAME + "/" + CONTAINER_NEO4J_PASSWORD)
                        .withEnv("apoc.export.file.enabled", "true")
                        .withEnv("apoc.import.file.enabled", "true")
                        .withEnv("apoc.import.file.use_neo4j_config", "true")
                        .withEnv("NEO4J_PLUGINS", "[\"apoc\"]")
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(CONTAINER_IMAGE)));
        container.setPortBindings(
                Arrays.asList(
                        String.format("%s:%s", HTTP_PORT, HTTP_PORT),
                        String.format("%s:%s", BOLT_PORT, BOLT_PORT)));
        Startables.deepStart(Stream.of(container)).join();
        LOGGER.info("container started");
        Awaitility.given()
                .ignoreExceptions()
                .await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(this::initConnection);
    }

    @Test
    public void testCreateNodeAllTypeField() {
        final Result checkExists = neo4jSession.run("MATCH (p:Person) RETURN p limit 1");
        if (checkExists.hasNext()) {
            neo4jSession.run("MATCH (p:Person) delete p");
        }

        Configuration configuration = Configuration.from(new File("src/test/resources/allTypeFieldNode.json"));
        Neo4jClient neo4jClient = Neo4jClient.build(configuration, null);

        neo4jClient.init();
        for (int i = 0; i < MOCK_NUM; i++) {
            neo4jClient.tryWrite(mockAllTypeFieldTestNode(neo4jClient.getNeo4jFields()));
        }
        neo4jClient.destroy();


        Result result = neo4jSession.run("MATCH (p:Person) return p");
        // nodes
        assertTrue(result.hasNext());
        int cnt = 0;
        while (result.hasNext()) {
            org.neo4j.driver.Record record = result.next();
            record.get("p").get("pbool").asBoolean();
            record.get("p").get("pstring").asString();
            record.get("p").get("plong").asLong();
            record.get("p").get("pshort").asInt();
            record.get("p").get("pdouble").asDouble();
            List list = (List) record.get("p").get("pstringarr").asObject();
            record.get("p").get("plocaldate").asLocalDate();
            cnt++;

        }
        assertEquals(cnt, MOCK_NUM);
    }


    /**
     * 创建关系 必须先有节点
     * 所以先创建节点再模拟关系
     */
    @Test
    public void testCreateRelation() {
        final Result checkExists = neo4jSession.run("MATCH (p1:Person)-[r:LINK]->(p1:Person) return r limit 1");
        if (checkExists.hasNext()) {
            neo4jSession.run("MATCH (p1:Person)-[r:LINK]->(p1:Person) delete r,p1,p2");
        }

        String createNodeCql = "create (p:Person) set p.id = '%s'";
        Configuration configuration = Configuration.from(new File("src/test/resources/relationship.json"));

        Neo4jClient neo4jClient = Neo4jClient.build(configuration, null);
        neo4jClient.init();
        //创建节点为后续写关系做准备
        //Create nodes to prepare for subsequent write relationships
        for (int i = 0; i < MOCK_NUM; i++) {
            neo4jSession.run(String.format(createNodeCql, i + "start"));
            neo4jSession.run(String.format(createNodeCql, i + "end"));
            Record record = new MockRecord();
            record.addColumn(new StringColumn(i + "start"));
            record.addColumn(new StringColumn(i + "end"));
            neo4jClient.tryWrite(record);

        }
        neo4jClient.destroy();

        Result result = neo4jSession.run("MATCH (start:Person)-[r:LINK]->(end:Person) return r,start,end");
        // relationships
        assertTrue(result.hasNext());
        int cnt = 0;
        while (result.hasNext()) {
            org.neo4j.driver.Record record = result.next();

            Node startNode = record.get("start").asNode();
            assertTrue(startNode.hasLabel("Person"));
            assertTrue(startNode.asMap().containsKey("id"));

            Node endNode = record.get("end").asNode();
            assertTrue(startNode.hasLabel("Person"));
            assertTrue(endNode.asMap().containsKey("id"));


            String name = record.get("r").type().name();
            assertEquals("RELATIONSHIP", name);
            cnt++;
        }
        assertEquals(cnt, MOCK_NUM);
    }

    /**
     * neo4j中,Label和关系类型,想动态的写，需要借助于apoc函数
     */
    @Test
    public void testUseApocCreateDynamicLabel() {
        List<String> dynamicLabel = new ArrayList<>();
        for (int i = 0; i < MOCK_NUM; i++) {
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
        assertEquals(cnt, MOCK_NUM);

    }


    private Record mockAllTypeFieldTestNode(List<Neo4jProperty> neo4JProperties) {
        Record mock = new MockRecord();
        for (Neo4jProperty field : neo4JProperties) {
            mock.addColumn(MockUtil.mockColumnByType(PropertyType.fromStrIgnoreCase(field.getType())));
        }
        return mock;
    }

    @After
    public void destroy() {
        if (neo4jSession != null) {
            neo4jSession.close();
        }
        if (neo4jDriver != null) {
            neo4jDriver.close();
        }
        if (container != null) {
            container.close();
        }
    }

    private void initConnection() {
        neo4jDriver =
                GraphDatabase.driver(
                        CONTAINER_URI,
                        AuthTokens.basic(CONTAINER_NEO4J_USERNAME, CONTAINER_NEO4J_PASSWORD));
        neo4jSession = neo4jDriver.session(SessionConfig.forDatabase("neo4j"));
    }
}
