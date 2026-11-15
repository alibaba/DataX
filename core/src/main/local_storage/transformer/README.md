# 第三方 Transformer UDF 

如果需要添加第三方或自定义 Transformer UDF 时，请在此目录新建以 **udf name** 命名的文件夹，将 jar 包放入新建的文件夹下，
同时在其文件夹下创建配置文件 `transformer.json`，并添加如下配置：
 
```json
{
    "class": "自定义的Transformer全限定类名",
    "name": "udf 的名字"
}
```

<br/>

# 例子
例如实现一个将 ipv4 字符串类型类型的地址转为数字的 Transformer UDF，可以这样实现

新建一个 Maven 项目，在项目下创建一个 lib 文件夹，从环境中考虑如下 jar 包到 lib 下
* datax-common-0.0.1-SNAPSHOT.jar
* datax-core-0.0.1-SNAPSHOT.jar
* datax-transformer-0.0.1-SNAPSHOT.jar

pom.xml 中添加如下依赖
```
        <!-- DataX Common -->
        <dependency>
            <groupId>com.alibaba.datax</groupId>
            <artifactId>datax-common</artifactId>
            <version>0.0.1-SNAPSHOT</version>
            <scope>system</scope>
            <systemPath>${project.basedir}/lib/datax-common-0.0.1-SNAPSHOT.jar</systemPath>
        </dependency>

        <!-- DataX Transformer -->
        <dependency>
            <groupId>com.alibaba.datax</groupId>
            <artifactId>datax-transformer</artifactId>
            <version>0.0.1-SNAPSHOT</version>
            <scope>system</scope>
            <systemPath>${project.basedir}/lib/datax-transformer-0.0.1-SNAPSHOT.jar</systemPath>
        </dependency>

        <!-- DataX Core -->
        <dependency>
            <groupId>com.alibaba.datax</groupId>
            <artifactId>datax-core</artifactId>
            <version>0.0.1-SNAPSHOT</version>
            <scope>system</scope>
            <systemPath>${project.basedir}/lib/datax-core-0.0.1-SNAPSHOT.jar</systemPath>
        </dependency>

```

创建一个 `Ipv4ToNum` 类，并继承 Transformer，实现 evaluate 方法
```java
package com.alibaba.datax.example;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.transport.transformer.TransformerErrorCode;
import com.alibaba.datax.transformer.Transformer;

/**
 * Convert ipv4 address to number
 * 
 */
public class Ipv4ToNum extends Transformer {

    public Ipv4ToNum() {
        setTransformerName("ipv42num");
    }

    public Record evaluate(Record record, Object... paras) {
        int columnIndex = (Integer) paras[0];
        Column column = record.getColumn(columnIndex);
        try {
            String oriValue = column.asString();

            //如果字段为空，返回 0
            if (oriValue==null) {
                oriValue = "0.0.0.0";
            }

            String[] seq = oriValue.split("\\.");
            long num = (Long.parseLong(seq[0]) << 24) + (Long.parseLong(seq[1]) << 16) + (Long.parseLong(seq[2]) << 8) + Long.parseLong(seq[3]);
            record.setColumn(columnIndex, new LongColumn(num));

        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(),e);
        }
        return record;
    }

}
```

在 `datax/local_storage/transformer/` 新建 **ipv42num** 文件夹，将上面编译后的 jar 包放入其中，

在 `datax/local_storage/transformer/ipv42num/transformer.json` 配置文件中添加如下配置 
```json
{
    "class": "com.alibaba.datax.example.Ipv4ToNum",
    "name": "ipv42num"
}
``` 


