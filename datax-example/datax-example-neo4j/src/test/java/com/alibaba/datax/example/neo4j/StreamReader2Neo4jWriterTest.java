package com.alibaba.datax.example.neo4j;

import com.alibaba.datax.example.ExampleContainer;
import com.alibaba.datax.example.util.PathUtil;
import org.junit.After;
import org.junit.Assert;
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

import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * {@code Author} FuYouJ
 * {@code Date} 2023/8/19 21:48
 */

public class StreamReader2Neo4jWriterTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamReader2Neo4jWriterTest.class);
    private static final String CONTAINER_IMAGE = "neo4j:5.9.0";

    private static final String CONTAINER_HOST = "neo4j-host";
    private static final int HTTP_PORT = 7474;
    private static final int BOLT_PORT = 7687;
    private static final String CONTAINER_NEO4J_USERNAME = "neo4j";
    private static final String CONTAINER_NEO4J_PASSWORD = "Test@12343";
    private static final URI CONTAINER_URI = URI.create("neo4j://localhost:" + BOLT_PORT);

    protected static final Network NETWORK = Network.newNetwork();

    private GenericContainer<?> container;
    protected Driver neo4jDriver;
    protected Session neo4jSession;
    private static final int CHANNEL = 5;
    private static final int READER_NUM = 10;

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
        if (neo4jSession.run(query).hasNext()) {
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
