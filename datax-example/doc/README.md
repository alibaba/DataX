## [DataX-Example]调试datax插件的模块

### 为什么要开发这个模块

一般使用DataX启动数据同步任务是从datax.py 脚本开始，获取程序datax包目录设置到系统变量datax.home里，此后系统核心插件的加载，配置初始化均依赖于变量datax.home,这带来了一些麻烦，以一次本地 DeBug streamreader 插件为例。

- maven 打包 datax 生成 datax 目录
- 在 IDE 中 设置系统环境变量 datax.home，或者在Engine启动类中硬编码设置datax.home。
- 修改插件 streamreader 代码
- 再次 maven 打包，使JarLoader 能够加载到最新的 streamreader 代码。
- 调试代码

在以上步骤中，打包完全不必要且最耗时,等待打包也最煎熬。

所以我编写一个新的模块(datax-example)，此模块特用于本地调试和复现 BUG。如果模块顺利编写完成，那么以上流程将被简化至两步。

- 修改插件 streamreader 代码。
- 调试代码

<img src="img/img01.png" alt="img" style="zoom:40%;" />

### 实现原理

- 不修改原有的ConfigParer,使用新的ExampleConfigParser,仅用于example模块。他不依赖datax.home,而是依赖ide编译后的target目录
- 将ide的target目录作为每个插件的目录类加载目录。

![img](img/img02.png)

### 如何使用 
1.修改插件的pom文件，做如下改动。以streamreader为例。<br/>
改动前
```xml
<build>
		<plugins>
			<!-- compiler plugin -->
			<plugin>
				<artifactId>maven-compiler-plugin</artifactId>
				<configuration>
					<source>${jdk-version}</source>
					<target>${jdk-version}</target>
					<encoding>${project-sourceEncoding}</encoding>
				</configuration>
			</plugin>
        </plugins>
</build>
```
改动后
```xml
<build>
    <resources>
        <!--将resource目录也输出到target-->
        <resource>
            <directory>src/main/resources</directory>
            <includes>
                <include>**/*.*</include>
            </includes>
            <filtering>true</filtering>
        </resource>
    </resources>
		<plugins>
			<!-- compiler plugin -->
			<plugin>
				<artifactId>maven-compiler-plugin</artifactId>
				<configuration>
					<source>${jdk-version}</source>
					<target>${jdk-version}</target>
					<encoding>${project-sourceEncoding}</encoding>
				</configuration>
			</plugin>
        </plugins>
</build>
```
#### 在example模块使用
1.在datax-example模块引入你需要的插件，默认只引入了streamreader、writer

2.打开datax-example的Main class

```java
public class Main {

    /**
     * 注意！
     * 1.在example模块pom文件添加你依赖的的调试插件，
     *   你可以直接打开本模块的pom文件,参考是如何引入streamreader，streamwriter
     * 2. 在此处指定你的job文件
     */
    public static void main(String[] args) {

        String classPathJobPath = "/job/stream2stream.json";
        String absJobPath = PathUtil.getAbsolutePathFromClassPath(classPathJobPath);
        startExample(absJobPath);
    }

    public static void startExample(String jobPath) {

        Configuration configuration = ExampleConfigParser.parse(jobPath);

        Engine engine = new Engine();
        engine.start(configuration);
    }

}
```
#### 在reader/writer模块使用
参考neo4jwriter的StreamReader2Neo4jWriterTest
```java
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

```